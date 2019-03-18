{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}

module Database.TDS.Login where

import           Database.TDS.Connection (newConnection)
import qualified Database.TDS.Proto as Proto
import qualified Database.TDS.Proto.Errors as Proto
import           Database.TDS.Types

import           Control.Exception
import           Control.Monad

import           Data.Monoid ((<>))
import qualified Data.Text as T

import           Network.HostName

import           System.Posix.Process

login :: Options -> IO Connection
login options = do
  c <- newConnection (options { _tdsOnMessage =
                                    \m -> case Proto.clsSeverity (Proto.messageClass m) of
                                            Proto.Information -> pure ()
                                            Proto.Fatal -> throwIO m })

  let prelogin = Proto.mkPacket
                   (Proto.mkPacketHeader Proto.PreLogin mempty)
                   (Proto.PreLoginP (Proto.versionOption 0 1 0 0 <>
                                     Proto.encryptionOff))

  getPreLoginResp <- tdsSendPacket c prelogin

  ResponseResultReceived preloginResp <- getPreLoginResp

  pid <- getProcessID
  hostname <- T.pack <$> getHostName

  let login7 = Proto.Login7P Proto.tdsVersion71
                             16384
                             (Proto.ClientProgVersion 0x00010000)
                             (Proto.ClientPID (fromIntegral pid))
                             (Proto.ConnectionID 0)
                             Proto.defaultLoginOptions
                             0
                             (Proto.Collation (Proto.LCID 0x0409) -- English US
                                              (Proto.CollationFlags 0)
                                              (Proto.CollationVersion 0))
                             hostname
                             (_tdsUser options)
                             (_tdsPassword options)
                             (_tdsAppName options)
                             "localhost"
                             ""
                             (_tdsClientName options)
                             "us_english"
                             (_tdsDatabase options)

                             -- TODO send MAC address
                             (Proto.ClientID 0 0)

                             "" "" "" 0
                             []
  getLogin7Resp <- tdsSendPacket c (Proto.mkPacket (Proto.mkPacketHeader Proto.Login7 mempty)
                                                   login7)
  ResponseResultReceived login7Resp <- getLogin7Resp

  pure c

-- login :: Options -> IO Connection
-- login options = do
--   conn <- asyncLogin options
--   waitUntilReady conn


