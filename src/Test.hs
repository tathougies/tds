{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GADTs #-}

import Database.TDS

import GHC.IO.Encoding

main :: IO ()
main = do
  setLocaleEncoding utf8 -- Needed for stack exec on nix

  let o = tdsOptionHost "172.17.0.1" <> tdsOptionPort 1401 <> tdsDebugLogging <>
          tdsOptionSecure <> tdsOptionUserAndPassword "SA" "beamTest!"
  conn <- login o

  connSt <- getReadyState conn
  putStrLn ("Connection is now in state " ++ show connSt)

  query conn "SELECT * FROM [Track]"

  pure ()

