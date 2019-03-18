{-# OPTIONS_GHC -funbox-strict-fields #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StandaloneDeriving #-}

module Database.TDS.Types where

import           Control.Applicative
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad.Identity

import           Data.Monoid ((<>))
import qualified Data.Text as T
import           Data.Word

import qualified Database.TDS.Proto as Proto
import qualified Database.TDS.Proto.Errors as Proto

import           System.IO

-- From MS-TDS reference:
--
-- https://msdn.microsoft.com/en-us/library/dd304523.aspx
-- Version 12/1/2017

-- | Client state type based on recommendation on page 120
data ClientState
    = Connecting
    | SentPreLogin
    | SentTLSSSLNegotiation
    | SentLogin7WithCompleteAuthenticationToken
    | SentLogin7WithSPNEGO
    | SentLogin7WithFAIR
    | LoggedIn
    | SentClientRequest
    | SentAttention
    | RoutingCompleted
    | Final
      deriving (Show, Enum, Eq, Ord, Bounded)

sendableInState :: Proto.PacketType sender resp d -> ClientState -> Bool
sendableInState Proto.PreLogin Connecting = True
sendableInState Proto.Login7   SentPreLogin = True
sendableInState Proto.TrMgrRequest LoggedIn = True
sendableInState Proto.SQLBatch LoggedIn = True
sendableInState Proto.Attention _ = True
sendableInState _ _ = False

stateTransition :: Proto.PacketType sender resp d -> ClientState -> ClientState
stateTransition Proto.PreLogin Connecting   = SentPreLogin
stateTransition Proto.Login7   SentPreLogin = SentLogin7WithCompleteAuthenticationToken
stateTransition Proto.SQLBatch LoggedIn     = SentClientRequest
stateTransition _ _ = Final

newtype ConnectionTimeout = ConnectionTimeout Int
    deriving (Show, Eq, Ord)

-- | Connection timeout in seconds
connectionTimeout :: ConnectionTimeout
connectionTimeout = ConnectionTimeout 15

-- * Protocol Errors

data TDSErrorType
    = TDSNoSuchHost | TDSSocketError
    | TDSServerUninitialized
    | TDSServerBusy | TDSServerQuit
    | TDSInvalidStateTransition
    | TDSInvalidResponse
    deriving Show

data TDSError where
    TDSError ::
        Show d =>
        { tdsErrorType    :: !TDSErrorType
        , tdsErrorState   :: !ClientState
        , tdsErrorRequest :: !(Maybe (Proto.Packet sender resp d Identity))
        , tdsErrorMessage :: !String
        } -> TDSError
deriving instance Show TDSError

tdsErrorNoReq :: TDSErrorType -> ClientState -> String -> TDSError
tdsErrorNoReq ty st =
    TDSError ty st
      (Nothing :: Maybe (Proto.Packet 'Proto.Client 'Proto.NoResponse () Identity))

instance Exception TDSError where
    displayException e =
        unlines ([ "TDS Error\n"
                 , "  " <> tdsErrorMessage e <> "\n"
                 , "  Type:  " <> show (tdsErrorType e)
                 , "  State: " <> show (tdsErrorState e) ] ++
                 case e of
                   TDSError { tdsErrorRequest = Nothing } -> []
                   TDSError { tdsErrorRequest = Just req } ->
                       [ "  Request: ", Proto.displayRequest req ])

-- * Options

data ConnectionInfo
    = ConnectionInfo
    { _tdsConnHost :: Maybe String
    , _tdsConnPort :: Maybe Word16
    } deriving (Show, Eq, Ord)

instance Monoid ConnectionInfo where
    mempty = ConnectionInfo Nothing Nothing
    mappend a b =
        ConnectionInfo
        { _tdsConnHost = _tdsConnHost b <|> _tdsConnHost a
        , _tdsConnPort = _tdsConnPort b <|> _tdsConnPort a
        }

instance Semigroup ConnectionInfo where
    (<>) = mappend

data TdsAuth = TdsAuth deriving (Show, Eq, Ord)

data Options
    = Options
    { _tdsOptionsSecure :: Bool

    , _tdsAuth          :: Maybe TdsAuth

    , _tdsConnInfo      :: ConnectionInfo

    , _tdsOnMessage     :: Proto.Message -> IO ()
    , _tdsOnEnvChange   :: Proto.EnvChange -> IO ()

    , _tdsUser          :: T.Text
    , _tdsPassword      :: T.Text
    , _tdsDatabase      :: T.Text
    , _tdsAppName       :: T.Text
    , _tdsClientName    :: T.Text
    }

instance Monoid Options where
    mempty =
        Options
        { _tdsOptionsSecure = False
        , _tdsAuth          = Nothing
        , _tdsConnInfo      = mempty
        , _tdsOnMessage     = \_ -> pure ()
        , _tdsOnEnvChange   = \_ -> pure ()

        , _tdsUser          = ""
        , _tdsPassword      = ""
        , _tdsDatabase      = ""
        , _tdsAppName       = ""
        , _tdsClientName    = ""
        }

    mappend a b =
        Options
        { _tdsOptionsSecure =
              _tdsOptionsSecure a || _tdsOptionsSecure b
        , _tdsAuth = _tdsAuth b <|> _tdsAuth a
        , _tdsConnInfo = _tdsConnInfo a <> _tdsConnInfo b
        , _tdsOnMessage = _tdsOnMessage a >> _tdsOnMessage b
        , _tdsOnEnvChange = _tdsOnEnvChange a >> _tdsOnEnvChange b

        , _tdsUser = altText (_tdsUser a) (_tdsUser b)
        , _tdsPassword = altText (_tdsPassword a) (_tdsPassword b)
        , _tdsDatabase = altText (_tdsDatabase a) (_tdsDatabase b)
        , _tdsAppName = altText (_tdsAppName a) (_tdsAppName b)
        , _tdsClientName = altText (_tdsClientName a) (_tdsClientName b)
        }
        where
          altText a b
              | T.null a = b
              | otherwise = a

instance Semigroup Options where
    (<>) = mappend

defaultOptions :: Options
defaultOptions =
    mempty
    { _tdsConnInfo = ConnectionInfo
                   { _tdsConnHost = Just "localhost"
                   , _tdsConnPort = Just 1433
                   }
    , _tdsClientName = "haskell-tds"
    }

tdsOptionSecure :: Options
tdsOptionSecure =
    mempty { _tdsOptionsSecure = True }

tdsOptionHost :: String -> Options
tdsOptionHost h = mempty { _tdsConnInfo = mempty { _tdsConnHost = Just h } }

tdsOptionPort :: Word16 -> Options
tdsOptionPort p = mempty { _tdsConnInfo = mempty { _tdsConnPort = Just p } }

tdsOptionUserAndPassword :: T.Text -> T.Text -> Options
tdsOptionUserAndPassword user pw = mempty { _tdsUser = user
                                          , _tdsPassword = pw }

tdsOptionDatabase :: T.Text -> Options
tdsOptionDatabase db = mempty { _tdsDatabase = db }

tdsDebugLogging :: Options
tdsDebugLogging = mempty { _tdsOnMessage = debugMsg
                         , _tdsOnEnvChange = debugEnvChange }
  where
    debugEnvChange c = hPutStrLn stderr ("[ENVCHANGE] " ++ show c)
    debugMsg     msg =
      do let level = case Proto.clsSeverity (Proto.messageClass msg) of
                       Proto.Information -> "[INFO    ] "
                       Proto.Fatal       -> "[FATAL   ] "

             Proto.SQLError msgCode = Proto.messageCode msg
             Proto.ErrorClass cls = Proto.messageClass msg

         hPutStr stderr . unlines . map (level <>) $
           [ "Message Code:   " ++ show msg
           , "Message Status: " ++ show (Proto.messageSt msg)
           , "Message Class:  " ++ show cls
           , ""
           , "   " ++ T.unpack (Proto.messageText msg)
           , ""
           , "Server name:    " ++ T.unpack (Proto.messageServerName msg)
           , "Process name:   " ++ T.unpack (Proto.messageProcName msg)
           , "Line Number:    " ++ show (Proto.messageLineNum msg)
           ]

-- * Connection object

data ResponseResult (ty :: Proto.ResponseType *) where
    ResponseResultCancelled :: ResponseResult ('Proto.ResponseType 'True a)
    ResponseResultReceived  :: Show a => a -> ResponseResult ('Proto.ResponseType cancelable a)
deriving instance Show (ResponseResult ty)

data CancelInfo (canCancel :: Bool) where
    NonCancelable :: CancelInfo 'False
    Cancelable :: TVar Bool -> TMVar () -> CancelInfo 'True

class MkCancelable (canCancel :: Bool) where
    mkCancelable ::  STM (CancelInfo canCancel)

instance MkCancelable 'False where
    mkCancelable = pure NonCancelable
instance MkCancelable 'True where
    mkCancelable = Cancelable <$> newTVar False <*> newEmptyTMVar

data Connection
    = Connection
    { tdsSendPacket        :: forall (sender :: Proto.Sender) (d :: *)
                                     (cancelable :: Bool) (res :: *)
                            . ( Proto.Payload d, Proto.Response res
                              , Proto.KnownBool cancelable
                              , MkCancelable cancelable )
                           => Proto.Packet 'Proto.Client
                                           ('Proto.ExpectsResponse ('Proto.ResponseType cancelable res))
                                           d Identity
                           -> IO (IO (ResponseResult ('Proto.ResponseType cancelable res)))
    , tdsCancel            :: IO ()
    , tdsQuit              :: IO ()

    , tdsConnectionState   :: !(TVar ClientState)

    , tdsConnectionOptions :: !Options
    }

getReadyState :: Connection -> IO ClientState
getReadyState c =
  atomically $
  readTVar (tdsConnectionState c)

-- | Returns 'True' if the client is connected to the server. 'False'
-- otherwise
isConnected :: Connection -> IO Bool
isConnected c =
  getReadyState c >>=
  \case
     Final -> pure False
     _     -> pure True

-- | Returns 'True' if the client is ready to send commands to the
-- server. 'False' otherwise
isReady :: Connection -> IO Bool
isReady c =
  getReadyState c >>=
  \case
     LoggedIn -> pure True
     _        -> pure False

-- | Returns 'True' if the server is busy processing a
-- request. 'False' otherwise
isBusy :: Connection -> IO Bool
isBusy c =
  getReadyState c >>=
  \case
     SentClientRequest -> pure True
     SentAttention     -> pure True
     _                 -> pure False

-- | Returns 'True' if the server is processing a request that can be
-- canceled. 'False' otherwise
isCancelable :: Connection -> IO Bool
isCancelable c =
  getReadyState c >>=
  \case
     SentClientRequest -> pure True
     _                 -> pure False
