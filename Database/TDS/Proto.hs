{-# OPTIONS_GHC -funbox-strict-fields #-}

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TupleSections #-}

module Database.TDS.Proto where

import           Database.TDS.Proto.Errors

import           Control.Exception (Exception)
import           Control.Monad.Identity
import           Control.Monad.Reader
import           Control.Monad.State
import           Control.Monad.Tardis
import           Control.Monad.Trans.Maybe

import           Data.Attoparsec.Binary
import qualified Data.Attoparsec.ByteString as A hiding (Done)
import qualified Data.Attoparsec.ByteString.Streaming as S
import           Data.Bits ( bit, (.|.), (.&.), xor
                           , shiftL, shiftR )
import qualified Data.ByteString as BS
import           Data.ByteString.Builder
import           Data.ByteString.Builder.Extra hiding (Done)
import qualified Data.ByteString.Internal as IBS
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Streaming as SBS
import           Data.Int
import           Data.Maybe
import           Data.Monoid
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Foreign as T
import           Data.Word
import qualified Data.Vector as V

import           Debug.Trace

import           Foreign.C.Types
import           Foreign.ForeignPtr (withForeignPtr)
import           Foreign.Marshal.Utils (copyBytes)
import           Foreign.Ptr
import           Foreign.Storable

import           Streaming (Compose(..))
import qualified Streaming as S
import qualified Streaming.Prelude as S
import qualified Streaming.Internal as S (inspect)

cancelPacketSize, maximumPayloadPacketSize :: CSize
cancelPacketSize = 1024
maximumPayloadPacketSize = 65536

pktHdrSz :: CSize
pktHdrSz = 8

data Sender
    = Client
    | Server
    deriving Show

data ResponseInfo a
    = ExpectsResponse (ResponseType a)
    | NoResponse
    deriving Show

data ResponseType a
    = ResponseType Bool {-^ Cancelable -} a
    deriving Show

data StreamType
    = TokenlessStream
    | TokenStream
    deriving Show

data Unimplemented

instance Show Unimplemented where
    show = error "Unimplemented"

-- * Misc data types

newtype SPID = SPID Word16
    deriving (Show, Eq)

-- * Packets

-- ** Packet header

-- | Data type representing valid packet types
data PacketType (sender :: Sender) (resp :: ResponseInfo *) (d :: *) where
  PreLogin      :: PacketType 'Client
                              ('ExpectsResponse ('ResponseType 'False PreLogin))
                              PreLogin

  Login7        :: PacketType 'Client ('ExpectsResponse ('ResponseType 'False Login7Ack)) Login7

  SQLBatch      :: PacketType 'Client ('ExpectsResponse ('ResponseType 'False RowResults)) T.Text
  BulkLoad      :: PacketType 'Client 'NoResponse Unimplemented
  RPC           :: PacketType 'Client 'NoResponse Unimplemented

  -- TODO response or something
  Attention     :: PacketType 'Client ('ExpectsResponse ('ResponseType 'False ())) ()

  TrMgrRequest  :: PacketType 'Client 'NoResponse Unimplemented

  -- Server requests
  TabularResult :: PacketType 'Server 'NoResponse a

deriving instance Show (PacketType sender resp d)

packetType :: PacketType sender resp d -> Word8
packetType PreLogin      = 0x12
packetType Login7        = 0x10
packetType SQLBatch      = 0x01
packetType BulkLoad      = 0x07
packetType RPC           = 0x03
packetType Attention     = 0x06
packetType TrMgrRequest  = 0x0E
packetType TabularResult = 0x04

newtype PacketStatus (s :: Sender) = PacketStatus Word8
    deriving Show

instance Semigroup (PacketStatus s) where
    (<>) = mappend

instance Monoid (PacketStatus s) where
    mempty = PacketStatus 0
    mappend (PacketStatus a) (PacketStatus b) =
        PacketStatus (a .|. b)

hasStatus :: PacketStatus s -> PacketStatus s -> Bool
hasStatus (PacketStatus sts) (PacketStatus lookup) =
    sts .&. lookup > 0

pktStatusEndOfMessage :: PacketStatus s
pktStatusEndOfMessage = PacketStatus (bit 0)

pktStatusIgnore, pktStatusResetConn, pktStatusResetConnSkipTran
    :: PacketStatus 'Client
pktStatusIgnore       = PacketStatus (bit 1)
pktStatusResetConn    = PacketStatus (bit 2)
pktStatusResetConnSkipTran = PacketStatus (bit 3)

newtype PacketSequenceID = PacketSequenceID Word8
    deriving (Show, Eq)

type family Packed (packed :: Bool) (a :: *) (b :: *) where
    Packed 'True a b = a
    Packed 'False a b = b

data PacketHeader (sender :: Sender) (resp :: ResponseInfo *) (d :: *) where
    PacketHeader ::
        { pktHdrType   :: !(PacketType   sender resp d)
        , pktHdrStatus :: !(PacketStatus sender)
        , pktHdrLength :: !Word16 {- Filled in automatically on send -}
        , pktHdrSPID   :: !SPID
        , pktHdrSeqID  :: !PacketSequenceID {- Filled in automatically on send -}
        , pktHdrWindow :: !Word8 -- == 0
        } -> PacketHeader sender resp d
deriving instance Show (PacketHeader sender resp d)

mkPacketHeader :: PacketType sender resp d -> PacketStatus sender
               -> PacketHeader sender resp d
mkPacketHeader ty flags = PacketHeader ty flags 0 (SPID 0)
                                       (PacketSequenceID 0) 0

writeHdr :: Ptr a -> PacketHeader 'Client resp d -> IO ()
writeHdr ptr (PacketHeader pktType (PacketStatus pktSts) len
                           (SPID spid16) (PacketSequenceID seqId)
                           window) =
  do poke (castPtr ptr) (packetType pktType)
     poke (castPtr ptr `plusPtr` 1) pktSts
     be16 (castPtr ptr `plusPtr` 2) len
     be16 (castPtr ptr `plusPtr` 4) spid16
     poke (castPtr ptr `plusPtr` 6) seqId
     poke (castPtr ptr `plusPtr` 7) window

  where
    be16 :: Ptr Word8 -> Word16 -> IO ()
    be16 ptr x =
      let lo, hi :: Word8
          lo = fromIntegral (x .&. 0xFF)
          hi = fromIntegral ((x `shiftR` 8) .&. 0xFF)
      in poke (castPtr ptr) hi >>
         poke (castPtr ptr `plusPtr` 1) lo

readHdr :: PacketType sender resp d
        -> Ptr a -> IO (Maybe (PacketHeader sender resp d))
readHdr pktTy ptr =
  do tag <- peek (castPtr ptr)
     if tag /= packetType pktTy
        then pure Nothing
        else Just <$>
             (PacketHeader pktTy
             <$> (PacketStatus <$> peek (castPtr ptr `plusPtr` 1))
             <*> be16 (ptr `plusPtr` 2)
             <*> (SPID <$> be16 (ptr `plusPtr` 4))
             <*> (PacketSequenceID <$> peek (castPtr ptr `plusPtr` 6))
             <*> peek (castPtr ptr `plusPtr` 7))
  where
    be16 ptr = do
      hi <- peek (castPtr ptr :: Ptr Word8)
      lo <- peek (castPtr ptr `plusPtr` 1 :: Ptr Word8)
      pure ((fromIntegral hi `shiftL` 8) .|. fromIntegral lo)


data Packet (sender :: Sender) (resp :: ResponseInfo *) (d :: *) (f :: * -> *) where
    Packet ::
        { pktHdr  :: !(PacketHeader sender resp d)
        , pktData :: !(f d)
        } -> Packet sender resp d f

mkPacket :: PacketHeader sender resp d -> d
         -> Packet sender resp d Identity
mkPacket hdr d = Packet hdr (Identity d)

packetEncoding :: Payload d
               => Packet sender resp d Identity
               -> Packet sender resp d PacketEncoding
packetEncoding (Packet hdr (Identity x)) =
    Packet hdr (PacketEncoding (encodePayload x))

deriving instance Show d => Show (Packet sender resp d Identity)
--deriving instance Show d => Show (Packet sender resp d Encoded)

displayRequest :: Show d => Packet sender resp d Identity -> String
displayRequest = show

-- * Packet payloads

data PayloadEncoder (streaming :: StreamType) where
    PayloadStreamEncoder :: Builder -> PayloadEncoder 'TokenStream
    PayloadBatchEncoder  :: Word -> Builder -> PayloadEncoder 'TokenlessStream

newtype PacketEncoding pld
    = PacketEncoding (PayloadEncoder (PayloadStreaming pld))

class Show pld => Payload pld where
    type PayloadStreaming pld :: StreamType

    encodePayload :: pld -> PayloadEncoder (PayloadStreaming pld)

instance Payload () where
    type PayloadStreaming () = 'TokenlessStream

    encodePayload _ = PayloadBatchEncoder 0 mempty

instance Payload Text where
    type PayloadStreaming Text = 'TokenlessStream

    encodePayload t = PayloadBatchEncoder (fromIntegral $ T.length t * 2)
                                          (byteString (TE.encodeUtf16LE t))

runBatchEncoder :: Monoid fixup
                => Tardis fixup (Builder, Word) ()
                -> PayloadEncoder 'TokenlessStream
runBatchEncoder action =
    let (_, (_, (builder, len))) = runTardis action (mempty, (mempty, 0))
    in PayloadBatchEncoder len builder

getFixups :: Tardis fixup (Builder, Word) fixup
getFixups = getFuture

getPosition :: Tardis fixup (Builder, Word) Word
getPosition = snd <$> getPast

fixup :: Monoid fixup
      => fixup -> Tardis fixup (Builder, Word) ()
fixup f = modifyBackwards (mappend f)

emit :: Word -> Builder -> Tardis fixup (Builder, Word) ()
emit len b = modifyForwards (\(before, p) -> (before <> b, len + p))

-- ** PRELOGIN Payload

newtype MajorVersion = MajorVersion Word8  deriving (Show, Num)
newtype MinorVersion = MinorVersion Word8  deriving (Show, Num)
newtype BuildNumber  = BuildNumber  Word16 deriving (Show, Num)
newtype SubBuildNumber = SubBuildNumber Word16 deriving (Show, Num)

data Encryption
    = EncryptionOff | EncryptionOn
    | EncryptionNotSupported
    | EncryptionRequired
    deriving Show

data Nonce
    = Nonce !Word64 !Word64 !Word64 !Word64
    deriving Show

data PreLoginOption
    = VersionOption         !MajorVersion !MinorVersion !BuildNumber !SubBuildNumber
    | EncryptionOption      !Encryption
    | InstanceNameOption    !Text
    | ThreadIdOption        !Word32
    | MarsOption            !Bool
--  TODO
--    | TraceIdOption         !UUID !UUID
    | FedAuthRequiredOption !Word8
    | NonceOption           !Nonce
    deriving Show

type PreLoginOptions = [ PreLoginOption ]

versionOption :: MajorVersion -> MinorVersion -> BuildNumber -> SubBuildNumber
              -> PreLoginOptions
versionOption maj min b sb =
    [ VersionOption maj min b sb ]

encryptionOff :: PreLoginOptions
encryptionOff = [ EncryptionOption EncryptionNotSupported ]

newtype PreLogin
    = PreLoginP
    { preLoginOptions :: PreLoginOptions
    } deriving Show

instance Payload PreLogin where
    type PayloadStreaming PreLogin = 'TokenlessStream

    encodePayload (PreLoginP options) =
        runBatchEncoder $
        do fixups <- getFixups

           emitFixups options fixups

           emit 1 (word8 0xFF) -- Termination token

           forM_ options $ \option ->
             do o  <- getPosition
                buildOption option
                o' <- getPosition
                fixup ([( fromIntegral o
                        , fromIntegral $ o' - o)])

      where
        emitFixups [] _ = pure ()
        emitFixups (option:options) ~(~(offset, len):fixups) =
            do emit 1 (word8 (optionToken option))
               emit 2 (word16BE offset)
               emit 2 (word16BE len)
               emitFixups options fixups

        optionToken :: PreLoginOption -> Word8
        optionToken VersionOption {}         = 0x00
        optionToken EncryptionOption {}      = 0x01
        optionToken InstanceNameOption {}    = 0x02
        optionToken ThreadIdOption {}        = 0x03
        optionToken MarsOption {}            = 0x04
--        optionToken TraceIdOption {}         = 0x05
        optionToken FedAuthRequiredOption {} = 0x06
        optionToken NonceOption {}           = 0x07

        buildOption (VersionOption (MajorVersion major)
                                   (MinorVersion minor)
                                   (BuildNumber build)
                                   (SubBuildNumber subBuild)) =
            emit 1 (word8 major) >> emit 1 (word8 minor) >>
            emit 2 (word16BE build) >> emit 2 (word16BE subBuild)
        buildOption (EncryptionOption enc) =
            buildEncryption enc
        buildOption (InstanceNameOption nm) =
            let nmBs = TE.encodeUtf8 nm
            in emit (fromIntegral $ BS.length nmBs)
                    (byteString nmBs) >>
               emit 0 (word8 0)
        buildOption (ThreadIdOption w) =
            emit 4 (word32BE w)
        buildOption (MarsOption False) = emit 1 (word8 0x00)
        buildOption (MarsOption True)  = emit 1 (word8 0x01)
        buildOption (FedAuthRequiredOption a) =
            emit 1 (word8 a)
        buildOption (NonceOption (Nonce a3 a2 a1 a0)) =
            emit 32 (word64LE a0 <> word64LE a1 <>
                     word64LE a2 <> word64LE a3)

        buildEncryption e =
            emit 1 . word8 $
            case e of
              EncryptionOff -> 0x00
              EncryptionOn  -> 0x01
              EncryptionNotSupported -> 0x02
              EncryptionRequired -> 0x03

-- ** LOGIN7 Payload

newtype TDSVersion
    = TDSVersion { fromTDSVersion :: Word32 }
    deriving Show
newtype ClientProgVersion
    = ClientProgVersion { fromClientProgVersion :: Word32 }
    deriving Show
newtype ClientPID
    = ClientPID { fromClientPID :: Word32 }
    deriving Show
newtype ConnectionID
    = ConnectionID { fromConnectionID :: Word32 }
    deriving Show
newtype Login7Options
    = Login7Options { fromLogin7Options :: Word32 }
    deriving Show

instance Monoid Login7Options where
    mempty = Login7Options 0
    mappend (Login7Options a) (Login7Options b) =
        Login7Options (a .|. b)

instance Semigroup Login7Options where
    (<>) = mappend

defaultLoginOptions :: Login7Options
defaultLoginOptions = Login7Options 0x000003e0

data Login7Feature = Login7Feature deriving Show

-- | 12-bit Windows language code identifier
newtype LCID = LCID Word16 deriving Show
newtype CollationFlags = CollationFlags Word8 deriving Show
-- | 4-bit collation version
newtype CollationVersion = CollationVersion Word8 deriving Show

data Collation
    = Collation
    { collationLCID    :: !LCID
    , collationFlags   :: !CollationFlags
    , collationVersion :: !CollationVersion
    } deriving Show

data ClientID
    = ClientID !Word16 !Word32
    deriving (Show, Eq)

tdsVersion71 :: TDSVersion
tdsVersion71 = TDSVersion 0x00000071

data Login7
    = Login7P
    { login7_tdsVersion    :: !TDSVersion
    , login7_packetSize    :: !Word32
    , login7_clientProgVer :: !ClientProgVersion
    , login7_clientPID     :: !ClientPID
    , login7_connectionID  :: !ConnectionID
    , login7_flags         :: !Login7Options
    , login7_clientTmZone  :: !Word32
    , login7_collation     :: !Collation

    , login7_hostName      :: !Text
    , login7_userName      :: !Text
    , login7_password      :: !Text
    , login7_appName       :: !Text
    , login7_serverName    :: !Text
    , login7_extension     :: !Text
    , login7_cltIntName    :: !Text
    , login7_language      :: !Text
    , login7_database      :: !Text
    , login7_clientID      :: !ClientID
    , login7_SSPI          :: !Text
    , login7_atchDbFile    :: !Text
    , login7_changePasswd  :: !Text
    , login7_SSPILong      :: !Word32

    , login7_extraFeatures :: [Login7Feature]
    } deriving Show

data Login7Fixups
    = Login7Fixups
    { login7_totalLen      :: !(Sum Word32)
    , login7_hostNameOfs   :: !Word16
    , login7_userNameOfs   :: !Word16
    , login7_passwordOfs   :: !Word16
    , login7_appNameOfs    :: !Word16
    , login7_serverNameOfs :: !Word16
    , login7_extensionOfs  :: !Word16
    , login7_cltIntNameOfs :: !Word16
    , login7_languageOfs   :: !Word16
    , login7_databaseOfs   :: !Word16
    , login7_SSPIOfs       :: !Word16
    , login7_atchDbFileOfs :: !Word16
    , login7_changePasswdOfs :: !Word16
    } deriving Show

instance Monoid Login7Fixups where
    mempty = Login7Fixups mempty 0 0 0 0 0 0 0 0 0 0 0 0
    mappend a b =
        Login7Fixups (login7_totalLen a <> login7_totalLen b)
                     (go login7_hostNameOfs)   (go login7_userNameOfs)
                     (go login7_passwordOfs)   (go login7_appNameOfs)
                     (go login7_serverNameOfs) (go login7_extensionOfs)
                     (go login7_cltIntNameOfs) (go login7_languageOfs)
                     (go login7_databaseOfs)   (go login7_SSPIOfs)
                     (go login7_atchDbFileOfs) (go login7_changePasswdOfs)
        where
          go f = if f b == 0 then f a else f b

instance Semigroup Login7Fixups where
    (<>) = mappend

instance Payload Login7 where
    type PayloadStreaming Login7 = 'TokenlessStream

    encodePayload d =
        runBatchEncoder $
        do ~(Sum totalLen) <- login7_totalLen <$> getFixups

           emit 4 (word32LE (fromIntegral totalLen))

           emit 4 (word32BE (fromTDSVersion (login7_tdsVersion d)))
           emit 4 (word32LE (login7_packetSize d))
           emit 4 (word32LE (fromClientProgVersion (login7_clientProgVer d)))
           emit 4 (word32LE (fromClientPID (login7_clientPID d)))
           emit 4 (word32LE (fromConnectionID (login7_connectionID d)))
           emit 4 (word32LE (fromLogin7Options (login7_flags d)))
           emit 4 (word32LE (login7_clientTmZone d))

           emitCollation (login7_collation d)

           emitOffset login7_hostName   login7_hostNameOfs
           emitOffset login7_userName   login7_userNameOfs
           emitOffset login7_password   login7_passwordOfs
           emitOffset login7_appName    login7_appNameOfs
           emitOffset login7_serverName login7_serverNameOfs
           emitOffset login7_extension  login7_extensionOfs
           emitOffset login7_cltIntName login7_cltIntNameOfs
           emitOffset login7_language   login7_languageOfs
           emitOffset login7_database   login7_databaseOfs

           let ClientID hi lo = login7_clientID d
           emit 4 (word32LE lo)
           emit 2 (word16LE hi)

           emitOffset login7_SSPI login7_SSPIOfs
           emitOffset login7_atchDbFile login7_atchDbFileOfs
--           emitOffset login7_changePasswd login7_changePasswdOfs

--           emit 4 (word32LE (login7_SSPILong d))

           curPos <- fromIntegral <$> getPosition
           fixup (mempty { login7_hostNameOfs = curPos })

           emitData id login7_hostName   (\x -> mempty { login7_hostNameOfs   = x })
           emitData id login7_userName   (\x -> mempty { login7_userNameOfs   = x })
           emitData enc login7_password   (\x -> mempty { login7_passwordOfs   = x })
           emitData id login7_appName    (\x -> mempty { login7_appNameOfs    = x })
           emitData id login7_serverName (\x -> mempty { login7_serverNameOfs = x })
           emitData id login7_extension  (\x -> mempty { login7_extensionOfs  = x })
           emitData id login7_cltIntName (\x -> mempty { login7_cltIntNameOfs = x })
           emitData id login7_language   (\x -> mempty { login7_languageOfs   = x })
           emitData id login7_database   (\x -> mempty { login7_databaseOfs   = x })

-- TODO TDS7.4 only
-- TODO           forM_ (login7_extraFeatures d) emitFeature

           --emit 1 (word8 0xFF)

           pos <- fromIntegral <$> getPosition
           fixup (mempty { login7_totalLen = Sum pos })
      where
        emitOffset getBs getOfs = do
          let bs = getBs d
          ofs <- getOfs <$> getFixups
          emit 2 (word16LE ofs)
          emit 2 (word16LE (fromIntegral (T.length bs)))

        -- | Password encryption algorithm as described on pg 64
        enc b = let swapped = ((b `shiftR` 4) .&. 0xF) .|.
                              ((b .&. 0xF) `shiftL` 4)
                in swapped `xor` 0xA5

        emitData f getBs setOfs = do
          let bs = getBs d
          if T.length bs > 0
             then do
               ofs <- fromIntegral <$> getPosition
               fixup (setOfs ofs)
               emit (2 * fromIntegral (T.length bs))
                    (byteString (BS.map f $ TE.encodeUtf16LE bs))
             else pure ()

        emitCollation (Collation (LCID lcid)
                                 (CollationFlags flags)
                                 (CollationVersion vers)) = do
          emit 4 (word32LE (((fromIntegral vers .&. 0xF) `shiftR` 28) .|.
                            (fromIntegral flags `shiftR` 20) .|.
                            fromIntegral lcid))


-- * Packet decoders

data ResponseDecoder (streaming :: StreamType) (res :: *) where
    DecodeBatchResponse :: (Ptr Word8 -> Word16 -> IO (Maybe res))
                        -> ResponseDecoder 'TokenlessStream res
    DecodeTokenStream :: (forall r. IO () -> S.Stream TokenStream IO r -> IO res)
                      -> ResponseDecoder 'TokenStream res

class Show res => Response res where
    type ResponseStreaming res :: StreamType

    responseDecoder :: ResponseDecoder (ResponseStreaming res) res

instance Response Unimplemented where
    type ResponseStreaming Unimplemented = 'TokenlessStream

    responseDecoder = error "responseDecoder{Unimplemented}"

type BatchDecoder = StateT (Ptr Word8, Word16) (ReaderT Word16 (MaybeT IO))

decodeBatch :: BatchDecoder a -> ResponseDecoder 'TokenlessStream a
decodeBatch decoder =
    DecodeBatchResponse $ \ptr sz ->
    runMaybeT (runReaderT (evalStateT decoder (ptr, 0)) sz)

read8 :: BatchDecoder Word8
read8 = do
  sz <- ask
  (res, ofs) <- get
  if ofs > sz
     then lift (lift (MaybeT (pure Nothing)))
     else do
       put (res, ofs + 1)

       liftIO (peek (res `plusPtr` fromIntegral ofs))

read16BE :: BatchDecoder Word16
read16BE = do
  hi <- fromIntegral <$> read8
  lo <- fromIntegral <$> read8
  pure ((hi `shiftL` 8) .|. lo)

read32BE :: BatchDecoder Word32
read32BE = do
  hi <- fromIntegral <$> read16BE
  lo <- fromIntegral <$> read16BE
  pure ((hi `shiftL` 16) .|. lo)

read64BE :: BatchDecoder Word64
read64BE = do
  hi <- fromIntegral <$> read32BE
  lo <- fromIntegral <$> read32BE
  pure ((hi `shiftL` 32) .|. lo)

read16LE :: BatchDecoder Word16
read16LE = do
  lo <- fromIntegral <$> read8
  hi <- fromIntegral <$> read8
  pure ((hi `shiftL` 8) .|. lo)

read32LE :: BatchDecoder Word32
read32LE = do
  lo <- fromIntegral <$> read16LE
  hi <- fromIntegral <$> read16LE
  pure ((hi `shiftL` 16) .|. lo)

read64LE :: BatchDecoder Word64
read64LE = do
  lo <- fromIntegral <$> read32LE
  hi <- fromIntegral <$> read32LE
  pure ((hi `shiftL` 32) .|. lo)

ztText :: Word16 -> BatchDecoder Text
ztText len = tell >>= go 0
  where
    go ofs start
      | ofs >= len = lift (lift (MaybeT (pure Nothing)))
      | otherwise  = do
          c <- read8
          if c == 0
             then finish start ofs
             else go (ofs + 1) start

    finish start finalLen = do
      (ptr, _) <- get
      liftIO (T.peekCStringLen (castPtr ptr `plusPtr` fromIntegral start,
                               fromIntegral finalLen))

tell :: BatchDecoder Word16
tell = snd <$> get

seek :: Word16 -> BatchDecoder ()
seek ofs = do
  sz <- ask
  if ofs < sz
     then modify (\(ptr, _) -> (ptr, ofs))
     else lift (lift (MaybeT (pure Nothing)))

-- ** PRELOGIN response

instance Response PreLogin where
    type ResponseStreaming PreLogin = 'TokenlessStream

    responseDecoder =
        decodeBatch $ do
          options <- readOptions id

          fmap (PreLoginP . catMaybes)
               (sequence options)

      where
        readOptions a = do
          tag <- read8
          if tag == (0xFF :: Word8)
             then pure (a [])
             else do
               ofs <- read16BE
               len <- read16BE
               readOptions (a . ((seek ofs >> readOption tag len):))

        readOption 0x00 len
          | len >= 6 =
              Just <$> (VersionOption
                       <$> (MajorVersion <$> read8)
                       <*> (MinorVersion <$> read8)
                       <*> (BuildNumber  <$> read16BE)
                       <*> (SubBuildNumber <$> read16BE))
        readOption 0x01 len
          | len >= 1 =
              fmap (Just . EncryptionOption) $ do
                tag <- read8
                case tag of
                  0x00 -> pure EncryptionOff
                  0x01 -> pure EncryptionOn
                  0x02 -> pure EncryptionNotSupported
                  0x03 -> pure EncryptionRequired
                  _    -> lift (lift (MaybeT (pure Nothing)))
        readOption 0x02 len
          | len >= 1 =
              Just . InstanceNameOption <$> ztText len
        readOption 0x03 len
          | len >= 4 =
              Just . ThreadIdOption <$> read32BE
        readOption 0x04 len
          | len >= 1 =
              Just . MarsOption <$> ((/=0) <$> read8)
        readOption 0x06 len
          | len >= 1 =
              Just . FedAuthRequiredOption <$> read8
        readOption 0x07 len
          | len == 32 =
              Just <$> (NonceOption <$>
                        (Nonce <$> read64LE <*> read64LE
                               <*> read64LE <*> read64LE))
        readOption _ _ = lift (lift (MaybeT (pure Nothing)))

data Login7Ack = Login7Ack !Word8 !TDSVersion !T.Text !ProgVersion
  deriving Show

instance Response Login7Ack where
    type ResponseStreaming Login7Ack = 'TokenStream

    responseDecoder = DecodeTokenStream $ \finish s ->
                      S.streamFold (\_ ack -> do
                                      S.liftIO finish
                                      case ack of
                                        Just ack' -> pure ack'
                                        _ -> fail "Token stream incomplete")
                                   (\x ack -> x >>= ($ ack))
                                   (\x ack ->
                                      case x of
                                        OneToken (LoginAck i v progNm progVers) next ->
                                            next (Just (Login7Ack i v progNm progVers))
                                        OneToken Done {} _
                                            | Just ack' <- ack ->
                                                S.liftIO finish >> pure ack'
                                            | otherwise -> fail "Premature DONE"
                                        OneToken token next -> do liftIO (putStrLn ("Unhandled token during LOGIN : " ++ show (() <$ token)))
                                                                  next ack
                                        ContParse {} -> fail "Can't parse unhandled token in LOGIN")
                                   s Nothing

-- * Tokens

newtype DoneSts = DoneSts Word16 deriving Show
newtype ProgVersion = ProgVersion Word32 deriving Show

data Message
    = Message
    { messageCode       :: !SQLError
    , messageSt         :: !Word8
    , messageClass      :: !ErrorClass
    , messageText       :: !Text
    , messageServerName :: !Text
    , messageProcName   :: !Text
    , messageLineNum    :: Word16
    } deriving Show
instance Exception Message

data EnvChange
    = EnvChangeDatabase !T.Text !T.Text
    | EnvChangeLanguage !T.Text !T.Text
    | EnvChangeCollation !BS.ByteString !BS.ByteString
    | EnvChangeUnknown
      deriving Show

data Token' f
    = TvpRow
    | Offset
    | ReturnStatus
    | ColMetadata !ColumnMetadata
    | AltMetadata
    | TableName
    | ColumnInfo
    | Order (V.Vector Word8)

    | Error !Message
    | Info !Message

    | ReturnValue
    | LoginAck !Word8 !TDSVersion !T.Text !ProgVersion
    | FeatureExtAck
    | Row !f
    | NbcRow
    | AltRow
    | EnvChange !EnvChange
    | SessionState
    | SSPI
    | FedAuthInfo
    | Done !DoneSts !Word16 !Word64 -- status, token, rowcount, where rowcount is 32-bit pre 7.2
    | DoneProc
    | DoneInProc
      deriving (Functor, Show)

type Token = Token' (SBS.ByteString IO ())

data TokenStream f
    = OneToken !Token f
    | ContParse !Token (SBS.ByteString IO () -> f)
      deriving Functor

doneHasMore :: DoneSts -> Bool
doneHasMore (DoneSts sts) =
    (sts .&. 1) /= 0

doneHasError :: DoneSts -> Bool
doneHasError (DoneSts sts) =
    (sts .&. 2) /= 0

parseTokenStream :: SBS.ByteString IO () -> S.Stream TokenStream IO ()
parseTokenStream bs =
    do (res, bs') <- liftIO (S.parse parseToken bs)
       case res of
         Left e ->
             fail ("Stream decode error: " ++ show e)
         Right (Right r) ->
           S.wrap (OneToken r (parseTokenStream bs'))
         Right (Left contParse) -> do
           S.wrap (ContParse (contParse bs') parseTokenStream)

usVarChar, bVarChar :: A.Parser (Int, Text)
bVarChar = do
  len <- fromIntegral <$> A.anyWord8
  (1 + len * 2,) <$> (TE.decodeUtf16LE <$> A.take (len * 2))

usVarChar = do
  len <- fromIntegral <$> anyWord16le
  (2 + len * 2,) <$> (TE.decodeUtf16LE <$> A.take (len * 2))

bVarByte :: A.Parser (Int, BS.ByteString)
bVarByte = do
  len <- fromIntegral <$> A.anyWord8
  (1 + len,) . BS.copy <$> A.take len

parseToken :: A.Parser (Either (SBS.ByteString IO () -> Token) Token)
parseToken =
    do tag <- A.anyWord8

       case tag of
         0x01 -> fail "TVPROW"
         0x78 -> fail "OFFSET"
         0x79 -> fail "RETURNSTATUS"
         0x81 -> Right . ColMetadata <$> colMetadataP
         0x88 -> fail "ALTMETADATA"
         0xA4 -> fail "TABNAME"
         0xA5 -> fail "COLINFO"
         0xA9 -> Right . Order <$> orderP
         0xAA -> Right . Error <$> messageP
         0xAB -> Right . Info  <$> messageP
         0xAC -> fail "RETURNVALUE"
         0xAD -> Right <$> loginAckP
         0xAE -> fail "FEATUREEXTACK"
         0xD1 -> pure (Left Row)
         0xD2 -> fail "NBCROW"
         0xD3 -> fail "ALTROW"
         0xE3 -> Right <$> envChangeP
         0xE4 -> fail "SESSIONSTATE"
         0xED -> fail "SSPI"
         0xEE -> fail "FEDAUTHINFO"

         -- Using TDS 7.1 for now
         0xFD -> fmap Right (Done <$> (DoneSts <$> anyWord16le) <*> anyWord16le
                                  <*> (fromIntegral <$> anyWord32le))
         0xFE -> fail "DONEPROC"
         0xFF -> fail "DONEINPROC"
         _    -> do
                  d <- BS.pack <$> replicateM 16 A.anyWord8
                  fail ("Unknown token in TDS stream: " ++ show tag ++ " " ++ show d)

  where
    nextPacket totalLen actualLength =
      if totalLen - fromIntegral actualLength < 0 then fail "len - actualLength < 0"
         else replicateM_ (fromIntegral $ totalLen - fromIntegral actualLength) A.anyWord8

    loginAckP = do
      len <- anyWord16le
      iface <-  A.anyWord8
      tdsVersion <- TDSVersion <$> anyWord32le

      (progByteLen, progName) <- bVarChar

      progVersion <- ProgVersion <$> anyWord32le

      let actualLength = 1 {- iface -} + 4 {- tdsVersion -} +
                         progByteLen {- progName -} +
                         4 {- progVersion -}

      nextPacket len actualLength

      pure (LoginAck iface tdsVersion progName progVersion)

    envChangeP = do
      len <- anyWord16le

      tag <- A.anyWord8
      (actualLen, chg) <-
        case tag of
          1 -> do
            (newDbLen, newDb) <- bVarChar
            (oldDbLen, oldDb) <- bVarChar
            pure (oldDbLen + newDbLen, EnvChangeDatabase oldDb newDb)
          2 -> do
            (newLangLen, newLang) <- bVarChar
            (oldLangLen, oldLang) <- bVarChar
            pure (oldLangLen + newLangLen, EnvChangeLanguage oldLang newLang)
          7 -> do
            (newCollLen, newColl) <- bVarByte
            (oldCollLen, oldColl) <- bVarByte
            pure (oldCollLen + newCollLen, EnvChangeCollation oldColl newColl)
          _ -> pure (0, EnvChangeUnknown)

      nextPacket len (1 + actualLen)

      pure (EnvChange chg)

    orderP = do
      len <- anyWord16le
      V.fromList <$> replicateM (fromIntegral len) A.anyWord8

    messageP = do
      len <- anyWord16le

      msgCode <- SQLError <$> anyWord32le
      st  <- A.anyWord8
      cls <- ErrorClass <$> A.anyWord8

      (msgByteLen, msgText)    <- usVarChar

      (serverNameByteLen, serverName) <- bVarChar
      (procNameByteLen, procName)     <- bVarChar
      lnNum      <- anyWord16le

      let actualLength = 4 {- errNum -} + 1 {- st -} + 1 {- cls -} +
                         msgByteLen {- msgText -} +
                         serverNameByteLen {- serverName -} +
                         procNameByteLen   {- procName -} +
                         2 {- lnNum -}

      nextPacket len actualLength

      pure (Message msgCode st cls msgText serverName procName lnNum)

-- * Tabular results

data EncryptionAlgorithm = EncryptionAlgorithm deriving Show
data EncryptionAlgorithmType = EncryptionAlgorithmType deriving Show

data CharKind = NormalChar | NationalChar deriving Show

data PrecScale = PrecScale !Word8 !Word8 deriving Show

data TypeLen = ByteLen | ShortLen deriving Show

data TypeInfo
    = NullType
    | IntNType !Bool !Word8
    | GuidType !Word8
    | DecimalType  !Bool !Word8 !PrecScale
    | NumericType  !Bool !Word8 !PrecScale
    | BitNType     !Bool !Word8
    | DecimalNType !Bool !Word8  !PrecScale
    | NumericNType !Bool !Word8  !PrecScale
    | FloatNType   !Bool !Word8
    | MoneyNType   !Bool !Word8
    | DtTmNType    !Bool !Word8
    | DateNType    !Bool !Word8
    | TimeNType    !Bool !Word8
    | DtTm2NType   !Word8
    | DtTmOfsType  !Word8
    | CharType     !TypeLen !CharKind !Word16 !(Maybe Collation)
    | VarcharType  !TypeLen !CharKind !Word16 !(Maybe Collation)
    | BinaryType   !Word16
    | VarBinType   !Word16
    | ImageType    !Word32
    | NTextType    !Word32 !Collation
    | SSVarType    !Word32
    | TextType     !Word32 !Collation
    | XMLType      !Word32
      deriving Show

data CryptoMetadata
    = CryptoMetadata
    { cryptoOrdinal  :: !Word16
    , cryptoUserType :: !Word32
    , cryptoBaseType :: !TypeInfo
    , cryptoEncAlgo  :: !EncryptionAlgorithm
    , cryptoAlgName  :: !T.Text
    , cryptoAlgType  :: !EncryptionAlgorithmType
    , cryptoNormVers :: !Word8
    } deriving Show

data ColumnData
    = ColumnData
    { cdUserType     :: !Word32 -- Usually just 16-bits, but upgraded to 32 in TDS 7.4
    , cdFlags        :: !Word16 -- TODO interpret this field
    , cdBaseTypeInfo :: !TypeInfo
    , cdTableName    :: !(Maybe T.Text)
    , cdCrypto       :: !(Maybe CryptoMetadata)
    , cdColName      :: !T.Text
    } deriving Show

data ColumnMetadata
    = ColumnMetadata
    { cmCekTbl  :: BS.ByteString
    , cmColData :: [ ColumnData ]
    } deriving Show

-- Defer data parsing to the end user. We only know enough about types to supply a proper length.
data RawColumn f where
    RawColumn :: ColumnData -> SBS.ByteString IO () -> (SBS.ByteString IO () -> f) -> RawColumn f
deriving instance Functor RawColumn

typeInfoParser :: A.Parser TypeInfo
typeInfoParser = do
  tag <- A.anyWord8
  case tag of
    0x1F -> pure $ NullType
    0x22 -> lLen $ ImageType
    0x23 -> collation $ lLen $ TextType
    0x24 -> bLen $ GuidType -- GUID
    0x25 -> bLen $ VarBinType
    0x26 -> bLen $ IntNType True -- INT(n)
    0x27 -> noColl $ bLen $ VarcharType ByteLen NormalChar
    0x28 -> bLen $ DateNType True
    0x29 -> bLen $ TimeNType True
    0x2A -> bLen $ DtTm2NType
    0x2B -> bLen $ DtTmOfsType
    0x2D -> bLen $ BinaryType
    0x2F -> noColl $ bLen $ CharType ByteLen NormalChar
    0x37 -> precScale $ bLen $ DecimalNType True  -- DECIMAL (legacy) is this 4?
    0x3F -> precScale $ bLen $ NumericType True -- NUMERIC (legacy)
    0x30 -> pure $ IntNType   False 1 -- TINYINT
    0x32 -> pure $ BitNType   False 1 -- BIT
    0x34 -> pure $ IntNType   False 2 -- SMALLINT
    0x38 -> pure $ IntNType   False 4 -- INT
    0x3A -> pure $ DtTmNType  False 4 -- SMALLDATETIME
    0x3B -> pure $ FloatNType False 4 -- REAL
    0x3C -> pure $ MoneyNType False 8 -- MONEY
    0x3D -> pure $ DtTmNType  True 8 -- DATETIME
    0x3E -> pure $ FloatNType False 8 -- FLOAT8
    0x62 -> lLen $ SSVarType
    0x63 -> collation $ lLen $ NTextType
    0x68 -> bLen $ BitNType True
    0x6A -> precScale $ bLen $ DecimalNType True
    0x6C -> precScale $ bLen $ NumericNType True
    0x6D -> bLen $ FloatNType True
    0x6E -> bLen $ MoneyNType True
    0x6F -> bLen $ DtTmNType True
    0x7A -> pure $ MoneyNType False 4 -- SMALLMONEY
    0x7F -> pure $ IntNType False 8 -- BIGINT
    0xA5 -> usLen $ VarBinType
    0xA7 -> optColl $ usLen $ VarcharType ShortLen NormalChar
    0xAD -> usLen $ BinaryType
    0xAF -> optColl $ usLen $ CharType ShortLen NormalChar
    0xE7 -> optColl $ usLen $ VarcharType ShortLen NationalChar
    0xEF -> optColl $ usLen $ CharType ShortLen NationalChar
    0xF1 -> usLen $ XMLType
    0xF0 -> fail "CLR UDT not supported"
    _ -> fail ("Unknown TypeInfo tag " ++ show tag)

  where
    bLen, usLen, lLen :: Num a => (a -> b) -> A.Parser b
    bLen mk = mk . fromIntegral <$> A.anyWord8
    usLen mk = mk . fromIntegral <$> anyWord16le
    lLen mk = mk . fromIntegral <$> anyWord32le

    precScale mk = do
      prec  <- A.anyWord8
      scale <- A.anyWord8
      mk <*> pure (PrecScale prec scale)

    collation mk =
      mk <*> collationP
    optColl mk =
      mk <*> (fmap Just collationP)
    noColl mk = mk <*> pure Nothing

    collationP = do
      lcidData <- anyWord32le

      let lcid = LCID (fromIntegral (lcidData .&. 0xFFFFF))
          colFlags = CollationFlags  (fromIntegral ((lcidData `shiftR` 20) .&. 0xFF))
          version = CollationVersion (fromIntegral ((lcidData `shiftR` 28) .&. 0xF))

      sortId <- A.anyWord8 -- TODO unused ?

      pure (Collation lcid colFlags version)

colDataParser :: A.Parser ColumnData
colDataParser = do
  userType <- fromIntegral <$> anyWord16le
  flags    <- anyWord16le
  typeInfo <- typeInfoParser

  let hasTableName =
          case typeInfo of
            NTextType {} -> True
            TextType  {} -> True
            ImageType {} -> True
            _ -> False

  tblName <- if hasTableName
             then fail "TODO table name parsing"
             else pure Nothing

  -- TODO Column encryption data

  (_, colNm) <- bVarChar

  pure (ColumnData userType flags typeInfo tblName Nothing colNm)

colMetadataP :: A.Parser ColumnMetadata
colMetadataP = do
  colCnt <- fromIntegral <$> anyWord16le

  -- Cek table not present until TDS 7.4

  let columnsPresent = if colCnt == 0xFFFF then 0 else colCnt

  colsData <- replicateM columnsPresent colDataParser

  if columnsPresent == 0
     then do -- No columns means we should have the informational 0xFFFF marker present
       metadata <- anyWord16le
       when (metadata /= 0xFFFF) (fail "TDS COLMETADATA decode error: Columns present")
       pure (ColumnMetadata mempty [])
     else
       pure (ColumnMetadata mempty colsData)

newtype RowResults
    = RowResults
    { getRowResults :: S.Stream (Compose (S.Of ColumnMetadata)
                                         (S.Stream (S.Stream RawColumn IO) IO))
                                IO ()
    }

instance Show RowResults where
    show _ = "RowResults <stream>"

instance Response RowResults where
    type ResponseStreaming RowResults = 'TokenStream

    responseDecoder = DecodeTokenStream (\finish s ->
                                             pure (RowResults (resultsDecoder s >>
                                                               liftIO finish)))
      where
        resultsDecoder :: S.Stream TokenStream IO a
                       -> S.Stream (Compose (S.Of ColumnMetadata)
                                            (S.Stream (S.Stream RawColumn IO) IO))
                                   IO ()
        resultsDecoder tokens =
            do res <- liftIO $ S.inspect tokens
               case res of
                 Left a -> pure () -- fail "Tokens ended prematurely"
                 Right (OneToken (ColMetadata mt) next) ->
                     S.wrap (Compose (mt S.:> parseRowData mt next))
                 Right (OneToken Done {} next) ->
                     pure () -- fail "Tokens ended before getting result"
                 Right (OneToken _ next) -> resultsDecoder next
                 Right (ContParse tok _) -> fail ("Can't parse " ++ show (() <$ tok))

        parseRowData :: ColumnMetadata
                     -> S.Stream TokenStream IO a
                     -> S.Stream (S.Stream RawColumn IO)
                                 IO
                                 (S.Stream
                                       (Compose
                                        (S.Of ColumnMetadata) (S.Stream (S.Stream RawColumn IO) IO))
                                       IO ())
        parseRowData cols tokens =
            do res <- liftIO $ S.inspect tokens
               case res of
                 Left a -> fail "Tokens ended while parsing row results"
                 Right (OneToken (Done sts _ _) next)
                     | doneHasMore sts -> pure (resultsDecoder next)
                     | otherwise       -> pure (pure ())
                 Right (OneToken _ next) -> parseRowData cols next
                 Right (ContParse (Row rowData) next) ->
                     S.wrap (parseColumns cols (cmColData cols) rowData next)
                 Right (ContParse tok _) -> fail ("Unknown token while parsing row result: " ++ show (() <$ tok))

        parseColumns :: ColumnMetadata -> [ColumnData]
                     -> SBS.ByteString IO ()
                     -> (SBS.ByteString IO () -> S.Stream TokenStream IO a)
                     -> S.Stream RawColumn IO
                          (S.Stream (S.Stream RawColumn IO) IO
                                (S.Stream (Compose
                                           (S.Of ColumnMetadata)
                                           (S.Stream (S.Stream RawColumn IO) IO))
                                           IO ()))
        -- Done parsing row data
        parseColumns mt [] rowData next =
            pure (parseRowData mt (next rowData))
        parseColumns mt (col:cols) rowData next =
            S.wrap (RawColumn col rowData (\row -> parseColumns mt cols row next))


-- * Splitting packets

data SplitEncoding sender resp pld
    = LastPacket (Ptr () -> IO CSize)
    | OnePacket (Ptr () -> IO (Packet sender resp pld (SplitEncoding sender resp)))

type SplitPacket sender resp pld =
    Packet sender resp pld (SplitEncoding sender resp)

splitPacket :: CSize -> Packet 'Client resp d PacketEncoding
            -> (Maybe CSize, SplitPacket 'Client resp d)
splitPacket bufSz (Packet pktHdr (PacketEncoding encoder)) =
    case encoder of
      PayloadBatchEncoder len builder ->
        let chunkSz = min bufSz (fromIntegral len)
            chunks = toLazyByteStringWith (untrimmedStrategy (fromIntegral chunkSz)
                                                     (fromIntegral chunkSz))
                                          mempty
                                          builder

            unfoldSplit _ _ [] = error "No data in packet"
            unfoldSplit i lenLeft (a:as) =
                let (ptr, ofs, len) = IBS.toForeignPtr a
                in Packet (pktHdr { pktHdrSeqID = PacketSequenceID (i + 1) }) $
                   if null as || len == lenLeft
                      then LastPacket $ \dst ->
                           withForeignPtr ptr $ \ptrRaw ->
                           copyBytes dst (castPtr ptrRaw `plusPtr` ofs) lenLeft >>
                           pure (fromIntegral lenLeft)
                      else OnePacket $ \dst ->
                           withForeignPtr ptr $ \ptrRaw ->
                           copyBytes dst (castPtr ptrRaw `plusPtr` ofs) (fromIntegral chunkSz) >>
                           pure (unfoldSplit (i + 1) (lenLeft - BS.length a)
                                             as)

        in ( Just chunkSz
           , unfoldSplit 0 (fromIntegral len) (BL.toChunks chunks) )
      PayloadStreamEncoder builder ->
        error "PayloadStreamEncoder TODO"

-- * KnownBool type class

class KnownBool (b :: Bool) where
    boolVal :: p b -> Bool

instance KnownBool 'True where
    boolVal _ = True
instance KnownBool 'False where
    boolVal _ = False
