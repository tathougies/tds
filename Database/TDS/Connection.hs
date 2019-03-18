{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module Database.TDS.Connection
    ( newConnection
    ) where

import           Database.TDS.Types
import           Database.TDS.Proto

import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Exception ( SomeException, IOException
                                   , Exception, throwIO, bracket
                                   , catch, finally, onException )
import           Control.Exception (mask)
import           Control.Monad.IO.Class
import           Control.Monad.Identity
import           Control.Monad.Trans

import           Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as IBS
import qualified Data.ByteString.Streaming as SBS
import           Data.Char
import           Data.Maybe (fromMaybe)
import           Data.Monoid ((<>))
import           Data.Proxy
import           Data.Text (Text)
import           Data.Word

import           Foreign.C.Types
import           Foreign.ForeignPtr
import           Foreign.Marshal.Alloc
import           Foreign.Ptr
import           Foreign.Storable

import qualified Network.Socket as BSD

import qualified Streaming as S
import qualified Streaming.Prelude as S

type family IsCancelable (resp :: ResponseInfo a) :: Bool where
    IsCancelable ('ExpectsResponse ('ResponseType r a)) = r
    IsCancelable _ = 'False

data SomeSentPacket where
    SomeSentPacket :: Show d
                   => Packet sender resp d Identity
                   -> CancelInfo (IsCancelable resp)
                   -> SomeSentPacket

data TDSCanceledException
    = TDSCanceled
    deriving Show
instance Exception TDSCanceledException

data TDSWasCanceledException
    = TDSWasCanceled
    deriving Show
instance Exception TDSWasCanceledException

data WriteEnd
    = WriteEnd !BSD.Socket

               -- Output buffer
               !(Maybe (ForeignPtr (), CSize))

data ReadEnd = ReadEnd !BSD.Socket

data ConnectionState
    = ConnectionQuit
    | ConnectionState
    { connectionWriteSock :: TVar (Either ThreadId WriteEnd)
    , connectionReadSock  :: TVar (Either ThreadId ReadEnd)

    , connectionCurReq    :: TVar (Maybe SomeSentPacket)

    , connectionCurState  :: TVar ClientState

    -- Information on current database, locale, etc
    , connectionEnvironment :: !ClientEnv
    }

closeWriteEnd :: WriteEnd -> IO ()
closeWriteEnd (WriteEnd _ _) = pure ()

closeReadEnd :: ReadEnd -> IO ()
closeReadEnd (ReadEnd s) = BSD.close s

-- | Start a new connection to a TDS server
newConnection :: Options -> IO Connection
newConnection opts = do
  -- TODO UNIX, pipe, or other transport possibilities
  let opts' = defaultOptions <> opts
      connInfo = _tdsConnInfo opts'

      addrInfo = BSD.defaultHints
                 { BSD.addrSocketType = BSD.Stream }

  addrs <- BSD.getAddrInfo (Just addrInfo) (_tdsConnHost connInfo)
                           (show <$> _tdsConnPort connInfo)
           `catch` \(e :: IOException) ->
                       throwIO (tdsErrorNoReq
                                  TDSNoSuchHost Connecting
                                  (show e))

  case addrs of
    [] -> throwIO (tdsErrorNoReq
                     TDSNoSuchHost Connecting
                     "No addresses returned by getAddrInfo")
    addr:_ ->
      do sock <- BSD.socket (BSD.addrFamily addr)
                            (BSD.addrSocketType addr)
                            (BSD.addrProtocol addr)
                   `catch` \(e :: IOException) ->
                               throwIO (tdsErrorNoReq
                                          TDSSocketError Connecting
                                          (show e))

         BSD.connect sock (BSD.addrAddress addr)
            `catch` (\(e :: IOException) ->
                         throwIO (tdsErrorNoReq TDSSocketError Connecting
                                                (show e)))
            `onException` BSD.close sock

         writeV <- newTVarIO (Right (WriteEnd sock Nothing))
         readV  <- newTVarIO (Right (ReadEnd sock))

         reqV  <- newTVarIO Nothing

         stV   <- newTVarIO Connecting

         let connSt = ConnectionState writeV readV reqV stV
                                      (clientEnvFromOptions opts)
         connStV <- newTVarIO connSt

         pure (Connection (sendPacket opts connStV)
                          (cancel     connStV)
                          (quit       connStV)
                          stV opts)

debugPtr :: Ptr a -> CSize -> IO ()
debugPtr _ _ = pure ()
-- debugPtr ptr' sz
--   | sz > width =
--       do putStrLn . foldMap formatHex =<< forM [0..width - 1] (peek . plusPtr ptr)
--          debugPtr (ptr' `plusPtr` width) (sz - width)
--   | otherwise =
--       putStrLn . foldMap formatHex =<< forM [0..fromIntegral sz - 1] (peek . plusPtr ptr)
-- 
--   where
--     width :: Num a => a
--     width = 8
-- 
--     ptr = castPtr ptr'
-- 
--     formatHex :: Word8 -> String
--     formatHex b =
--         let hi = fromIntegral ((b `shiftR` 4) .&. 0xF)
--             lo = fromIntegral (b .&. 0xF)
--         in intToDigit hi : intToDigit lo : ' ' : []

sendPackets :: BSD.Socket -> ForeignPtr () -> CSize
            -> SplitPacket 'Client resp d -> IO ()
sendPackets sock fPtr bufSz =
    withForeignPtr fPtr . go
  where
    go pkt ptr =
      case pktData pkt of
        LastPacket writePkt ->
          do pktSz <- writePkt (ptr `plusPtr` fromIntegral pktHdrSz)
             let totalSz = fromIntegral (pktSz + pktHdrSz)

             let hdr = pktHdr pkt
             writeHdr ptr (hdr { pktHdrStatus = pktHdrStatus hdr <>
                                                pktStatusEndOfMessage
                               , pktHdrLength = fromIntegral totalSz })

             debugPtr ptr totalSz

             BSD.sendBuf sock (castPtr ptr) (fromIntegral totalSz)
                `catch` \(e :: SomeException) -> do
                           putStrLn ("Exception " ++ show e)
                           error "Bad"

             pure ()
        OnePacket writePkt ->
          do pkt' <- writePkt (ptr `plusPtr` fromIntegral pktHdrSz)

             let hdr = pktHdr pkt
             writeHdr ptr (hdr { pktHdrLength = fromIntegral bufSz })

             debugPtr ptr bufSz
             BSD.sendBuf sock (castPtr ptr) (fromIntegral bufSz)

             go pkt' ptr

recvExactly :: BSD.Socket -> Ptr a -> Word16 -> IO ()
recvExactly sock p sz = do
  recvd <- fromIntegral <$> BSD.recvBuf sock (castPtr p) (fromIntegral sz)
  if recvd == sz
     then pure ()
     else recvExactly sock (p `plusPtr` fromIntegral recvd) (sz - recvd)

dataStream :: ReadEnd -> CancelInfo resp -> SBS.ByteString IO ()
dataStream (ReadEnd sock) cancel = do
  hdr <-
    liftIO . alloca $ \hdrP ->
      recvExactly sock (hdrP :: Ptr Word64) 8 >>
      readHdr (TabularResult :: PacketType 'Server 'NoResponse ()) hdrP

  case hdr of
    Nothing -> fail "Invalid header"
    Just hdr' -> do
      let pktLength = pktHdrLength hdr' - 8

          bufSz = 65536 - 8

          getChunk :: Word16 -> SBS.ByteString IO ()
          getChunk 0 = pure ()
          getChunk len = do
            case cancel of
              Cancelable isCanceled sync -> do
                cancelSt <- lift (atomically (readTVar isCanceled))
                when cancelSt (liftIO (throwIO TDSWasCanceled))
              _ -> pure ()

            chunk <-
              liftIO $ bracket startRead (\_ -> endRead) $ \_ -> do
                fPtr <- mallocForeignPtrBytes (fromIntegral len)
                actuallyRead <-
                    withForeignPtr fPtr $ \ptr -> do
                      bytesRead <- BSD.recvBuf sock ptr (fromIntegral len)
                      debugPtr ptr (fromIntegral bytesRead)
                      pure bytesRead
                pure (IBS.fromForeignPtr fPtr 0 actuallyRead)

            SBS.chunk chunk

            getChunk (len - fromIntegral (BS.length chunk))

          (startRead, endRead) =
            case cancel of
              Cancelable _ sync ->
                (atomically $ takeTMVar sync,
                 atomically $ putTMVar sync ())
              _ -> (pure (), pure ())

      liftIO endRead
      getChunk pktLength

      if pktHdrStatus hdr' `hasStatus` pktStatusEndOfMessage
         then pure ()
         else dataStream (ReadEnd sock) cancel

withConnectionState :: TVar ConnectionState -> STM a
                    -> (ConnectionState -> STM a)
                    -> STM a
withConnectionState stV onQuit onState = do
  connSt <- readTVar stV
  case connSt of
    ConnectionQuit -> onQuit
    ConnectionState {} -> onState connSt

waitUntilSendable :: ThreadId -> ConnectionState
                  -> (ClientState -> Bool)
                  -> STM (Either TDSError (WriteEnd, ReadEnd))
waitUntilSendable _ ConnectionQuit _ = retry
waitUntilSendable threadId
                  (ConnectionState { connectionWriteSock = writeEndV
                                   , connectionReadSock  = readEndV
                                   , connectionCurState  = stateV
                                   , connectionCurReq    = reqV })
                  canSendInState =
  do state <- readTVar stateV
     case state of
       _ | canSendInState state ->
             do writeSock <- either (\_ -> retry) pure =<<
                             readTVar writeEndV
                readSock  <- either (\_ -> retry) pure =<<
                             readTVar readEndV
                maybe (pure ()) (\_ -> retry) =<<
                  readTVar reqV

                writeTVar writeEndV (Left threadId)
                writeTVar readEndV  (Left threadId)

                pure (Right (writeSock, readSock))
       SentClientRequest ->
          tdsError TDSServerBusy SentClientRequest
                   "Can't send request while server is still processing"
       SentAttention -> retry
       Final ->
           tdsError TDSServerQuit Final "Connection is closing"
       _ -> tdsError TDSServerUninitialized Final "The connection is not yet ready"

  where
    tdsError ty st msg = pure (Left (tdsErrorNoReq ty st msg))

surrenderWrite :: TVar ConnectionState -> WriteEnd -> STM ()
surrenderWrite stV we = do
  st <- readTVar stV
  case st of
    ConnectionQuit -> pure ()
    ConnectionState { connectionWriteSock = writeEndV } ->
        do we' <- readTVar writeEndV
           case we' of
             Left {} -> writeTVar writeEndV (Right we)

             -- TODO output warning or something
             _       -> pure ()


sendPacket :: forall cancelable r d
            . ( Payload d, Response r, MkCancelable cancelable
              , KnownBool cancelable )
           => Options -> TVar ConnectionState
           -> Packet 'Client ('ExpectsResponse ('ResponseType cancelable r)) d Identity
           -> IO (IO (ResponseResult ('ResponseType cancelable r)))
sendPacket options stV pkt =
  myThreadId >>= go
  where
    go threadId =
      mask $ \unmask ->
      join . atomically .
      withConnectionState stV
        (pure (throwIO (tdsErrorNoReq TDSServerQuit Final
                    "Can't send request to closed connection"))) $ \st ->
      do sock <- waitUntilSendable threadId st (sendableInState (pktHdrType (pktHdr pkt)))
         case sock of
           Left err -> pure (throwIO err)
           Right (writeEnd, readEnd) -> do
               cancel <- mkCancelable
               writeTVar (connectionCurReq st) (Just (SomeSentPacket pkt cancel))

               pure (unmask $
                     let go = doSend writeEnd readEnd cancel

                         go' = if boolVal (Proxy :: Proxy cancelable)
                               then go `catch`
                                    \(e :: TDSCanceledException) ->
                                        cancelRequest st writeEnd >>
                                        throwIO e
                               else go

                         internalError =
                             join . atomically $ quitSTM' stV writeEnd readEnd

                     in go' `onException` internalError)

    encoding = packetEncoding pkt

    doSend we@(WriteEnd sock buf) readEnd cancel = do
      let (maxSz, splitEncoding) =
              splitPacket (maybe maximumPayloadPacketSize snd buf - pktHdrSz) encoding

      (buf', sz') <-
          case buf of
            Nothing ->
              case maxSz of
                Nothing -> fail "Don't know what size of buffer"
                Just maxSz' ->
                  (, maxSz') <$> mallocForeignPtrBytes (fromIntegral (maxSz' + pktHdrSz))
            Just (buf, sz) -> pure (buf, sz)

      sendPackets sock buf' sz' splitEncoding

      join . atomically . withConnectionState stV
             (pure (throwIO (TDSError TDSInvalidStateTransition Final (Just pkt)
                                      "Server quit before response received"))) $ \st ->
        do surrenderWrite stV we

           oldSt <- readTVar (connectionCurState st)
           let st' = stateTransition (pktHdrType (pktHdr pkt)) oldSt
           writeTVar (connectionCurState st) st'

           disconnectOnFinal st' we readEnd
             (throwIO (TDSError TDSInvalidStateTransition st' (Just pkt)
                         "The state transitioned to Final before a response could be received"))
             (pure (getResult readEnd cancel))

    getResult readEnd@(ReadEnd sock) cancel =
      case responseDecoder :: ResponseDecoder (ResponseStreaming r) r of
        DecodeBatchResponse decode -> do
          hdr <- alloca $ \hdrP ->
                 do recvExactly sock (hdrP :: Ptr Word64) 8
                    readHdr (TabularResult :: PacketType 'Server 'NoResponse r) hdrP

          case hdr of
            Nothing -> invalidResponse readEnd
            Just hdr'
              | not (pktHdrStatus hdr' `hasStatus` pktStatusEndOfMessage) ->
                  fail "Cannot decode batch message split over multiple packets"
              | otherwise ->
                  allocaBytes (fromIntegral $ pktHdrLength hdr') $ \pktBuf ->
                    do recvExactly sock pktBuf (pktHdrLength hdr' - 8)

                       debugPtr pktBuf (fromIntegral $ pktHdrLength hdr' - 8)

                       resp <- decode pktBuf (pktHdrLength hdr')
                       case resp of
                         Nothing -> fail "Invalid response"
                         Just resp' -> do
                           atomically . withConnectionState stV (pure ()) $ \st -> do
                             writeTVar (connectionReadSock st) (Right readEnd)
                             writeTVar (connectionCurReq st) Nothing
                           pure (ResponseResultReceived resp')

        DecodeTokenStream streamDecoder ->
          do let tokenStream = parseTokenStream (dataStream readEnd cancel)
                 validTokens s = do
                   res <- S.lift (S.inspect s)
                   case res of
                     Left a -> pure a
                     Right (OneToken tok next) ->
                       do includeToken <- S.lift (handleToken pkt options stV tok)
                          if includeToken
                             then S.wrap (OneToken tok (validTokens next))
                             else validTokens next
                     Right (ContParse tok next) ->
                       S.wrap (ContParse tok (validTokens . next))

                 finishUp = atomically . withConnectionState stV (pure ()) $
                            \st -> do
                              writeTVar (connectionCurState st) LoggedIn
                              writeTVar (connectionReadSock st) (Right readEnd)
                              writeTVar (connectionCurReq st) Nothing

             -- TODO statically determine the kind of DONE message to
             -- expect as the end of this stream
             --
             -- TODO if this throws an exception, we should close the connection
             res <- streamDecoder finishUp (validTokens tokenStream)

             pure (ResponseResultReceived res)

    invalidResponse :: ReadEnd -> IO (ResponseResult ('ResponseType cancelable r))
    invalidResponse readEnd =
      join . atomically . withConnectionState stV
        (pure $ throwIO (TDSError TDSInvalidResponse Final
                                  (Just pkt)
                                  "Invalid response received, but we've already quit")) $
      \st -> do
        clientSt <- readTVar (connectionCurState st)

        writeTVar (connectionCurState st) Final
        writeTVar (connectionReadSock st) (Right readEnd)
        quitSTM stV

        pure (throwIO (TDSError TDSInvalidResponse clientSt
                                (Just pkt)
                                "Invalid response received"))

    disconnectOnFinal :: ClientState -> WriteEnd -> ReadEnd
                      -> IO a -> IO a
                      -> STM (IO a)
    disconnectOnFinal Final we re failer _ = quitSTM' stV we re >> pure failer
    disconnectOnFinal _ _ _ _ action = pure action

    cancelRequest st writeEnd@(WriteEnd sock buf) = do
      curReq <- atomically (readTVar (connectionCurReq st))

      case curReq of
        Just (SomeSentPacket (Packet (PacketHeader { pktHdrType = SQLBatch }) _)
                             (Cancelable signal sync)) -> do
          let bufSz = cancelPacketSize
          buf <- mallocForeignPtrBytes (fromIntegral bufSz)

          atomically (writeTVar signal True)
          atomically (takeTMVar sync)

          let (_, splitEncoding) = splitPacket bufSz
                                     (Packet (mkPacketHeader Attention pktStatusEndOfMessage)
                                             (PacketEncoding (encodePayload ())))
          sendPackets sock buf bufSz splitEncoding

          -- TODO create read end, and read until done token
          let tokenStream = parseTokenStream (dataStream (ReadEnd sock) NonCancelable)
          readUntilDoneToken tokenStream

          -- Cancel all
          atomically $ do
            writeTVar (connectionCurState st) LoggedIn
            writeTVar (connectionReadSock st) (Right (ReadEnd sock))
            writeTVar (connectionCurReq st) Nothing

        _ -> pure ()

      -- Spawn a new thread to handle the shutdown
--      void . forkIO $ do
--        -- Sending a cancel request in the middle of a send operation
--        -- This just means sending the packet header with the EOM bit set
--
--        -- TODO sendPackets sock _ (pkt { pktData = LastPacket (\_ -> pure 0) })
--
--        atomically $ do
--          writeTVar sockV (Just writeEnd)
--          writeTVar sentV Nothing
--
--          -- We should be back in the old state
--
--        fail "TODO figure out cancel while sending request"

handleToken :: Packet clientServer respType d f
            -> Options -> TVar ConnectionState
            -> Token -> IO Bool
handleToken _ options stV (Info msg)      = False <$ _tdsOnMessage options msg
handleToken _ options stV (Error msg)     = False <$ _tdsOnMessage options msg
handleToken _ options stV (EnvChange chg) =
  join . atomically $ do
    st <- readTVar stV
    case st of
      ConnectionQuit -> pure (pure False)
      ConnectionState {} ->
        do let st' = st { connectionEnvironment = updateEnv chg (connectionEnvironment st) }
           writeTVar stV st'
           pure (False <$ _tdsOnEnvChange options chg)

handleToken pkt options stV (LoginAck {})
  | Login7 <- pktHdrType (pktHdr pkt) =
      atomically $ do
        st <- readTVar stV
        case st of
          ConnectionQuit -> pure True
          ConnectionState { connectionCurState = protoStV } ->
            do protoSt <- readTVar protoStV
               let setLoggedIn = writeTVar protoStV LoggedIn
               case protoSt of
                 SentLogin7WithCompleteAuthenticationToken -> setLoggedIn
                 SentLogin7WithSPNEGO -> setLoggedIn
                 SentLogin7WithFAIR -> setLoggedIn
                 _ -> pure ()
               pure True
handleToken _ _ _ _ = pure True

cancel :: TVar ConnectionState -> IO ()
cancel _ = fail "Cancel"

quitSTM :: TVar ConnectionState -> STM (IO ())
quitSTM stV = do
  withConnectionState stV (pure (pure ())) $ \st -> do
    we <- either (\_ -> retry) pure =<<
          readTVar (connectionWriteSock st)
    re <- either (\_ -> retry) pure =<<
          readTVar (connectionReadSock st)

    quitSTM' stV we re

quitSTM' :: TVar ConnectionState -> WriteEnd -> ReadEnd -> STM (IO ())
quitSTM' stV we re = do
  withConnectionState stV (pure (pure ())) $ \st -> do
    writeTVar stV ConnectionQuit

    pure (closeWriteEnd we >> closeReadEnd re)

quit :: TVar ConnectionState -> IO ()
quit = join . atomically . quitSTM

-- * Client environment

data ClientEnv
    = ClientEnv
    { clientEnvDatabase :: Text
    , clientEnvLanguage :: Text
    } deriving Show

-- TODO This should work
clientEnvFromOptions :: Options -> ClientEnv
clientEnvFromOptions _ = ClientEnv "master" "us_english"

updateEnv :: EnvChange -> ClientEnv -> ClientEnv
updateEnv (EnvChangeDatabase _ new) env = env { clientEnvDatabase = new }
updateEnv _ env = env

readUntilDoneToken :: S.Stream TokenStream IO () -> IO ()
readUntilDoneToken s = do
  res <- S.inspect s
  case res of
    Left {} -> pure ()
    Right (OneToken Done {} s') -> readUntilDoneToken s'
    Right (ContParse Done {} cont) -> pure ()
    Right (ContParse _ f) -> fail "Can't read (TODO)"
