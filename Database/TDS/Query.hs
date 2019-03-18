{-# LANGUAGE GADTs #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.TDS.Query where

import qualified Database.TDS.Proto as Proto
import           Database.TDS.Types

import           Control.Exception ( Exception, SomeException(..)
                                   , bracket, onException
                                   , throwIO, catch, mask )

import           Data.Bifunctor
import           Data.Bits
import qualified Data.ByteString.Streaming as SBS
import qualified Data.ByteString.Internal as IBS
import           Data.Foldable
import           Data.Maybe
import           Data.Ratio
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as T
import           Data.Word

import           Debug.Trace

import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.Storable

import           Numeric

import           System.IO
import qualified Streaming as S
import qualified Streaming.Prelude as S

newtype MsSQLRuntimeError = MsSQLRuntimeError T.Text
    deriving Show
instance Exception MsSQLRuntimeError

withTransaction :: Connection -> IO a -> IO a
withTransaction conn go =
    mask $ \unmask -> do
      beginTransaction conn
      res <- (unmask (fmap Right go <*
                commitTransaction conn)
                `catch` (\e@(MsSQLRuntimeError {}) -> pure (Left e)))
                `catch` (\e@(SomeException {}) -> hPutStrLn stderr ("Caught exception " ++ show e) >> rollbackTransaction conn >> throwIO e)
      case res of
        Right x -> pure x
        Left err -> throwIO err

execNoRows :: Connection -> T.Text -> IO ()
execNoRows tds q = do
  let sql = Proto.mkPacket (Proto.mkPacketHeader Proto.SQLBatch mempty) q
  getRes <- tdsSendPacket tds sql
  ResponseResultReceived (Proto.RowResults rows) <- getRes
  res <- S.inspect rows
  case res of
    Left () -> pure ()
    Right {} -> throwIO (MsSQLRuntimeError (T.pack ("Expected no rows for statement: " ++ T.unpack q)))

beginTransaction, commitTransaction, rollbackTransaction :: Connection -> IO ()
beginTransaction tds = execNoRows tds "BEGIN TRANSACTION"
commitTransaction tds = execNoRows tds "COMMIT TRANSACTION"
rollbackTransaction tds = execNoRows tds "ROLLBACK TRANSACTION"

query :: Connection -> T.Text -> IO ()
query conn sqlTxt = do
  let sqlBatch = Proto.mkPacket (Proto.mkPacketHeader Proto.SQLBatch mempty)
                                sqlTxt

  getRes <- tdsSendPacket conn sqlBatch

  ResponseResultReceived (Proto.RowResults rows) <- getRes

  S.mapsM_ (\(S.Compose (cols S.:> rows')) -> do
              S.mapsM_ (\row -> S.mapsM_ (\(Proto.RawColumn columnType columnData next) -> do
                                              bs' <- S.liftIO (printColumn columnType columnData)
                                              pure (next bs')) row) rows')
           rows

  pure ()

take8 :: Monad m => SBS.ByteString m () -> m (Word8, SBS.ByteString m ())
take8 bs = do
  r <- SBS.uncons bs
  case r of
    Nothing -> fail "take8: no more bytes"
    Just (r, bs') -> pure (r, bs')

take16LE :: S.MonadIO m => SBS.ByteString m () -> m (Word16, SBS.ByteString m ())
take16LE bs = do
  a S.:> bs' <- SBS.toStrict (SBS.splitAt 2 bs)
  let (fPtr, ofs, _) = IBS.toForeignPtr a
  x <- S.liftIO . withForeignPtr fPtr $ \ptr ->
       peek (ptr `plusPtr` ofs)
  pure (x, bs')
--  (lo, bs')  <- first fromIntegral <$> take8 bs
--  (hi, bs'') <- first fromIntegral <$> take8 bs'
--  pure ((hi `shiftL` 8) .|. lo, bs'')

takeLength :: S.MonadIO m => Proto.TypeLen -> SBS.ByteString m () -> m (Word16, SBS.ByteString m ())
takeLength Proto.ShortLen = take16LE
takeLength Proto.ByteLen = fmap (first fromIntegral) . take16LE

takeLE :: Monad m => Int -> SBS.ByteString m () -> m (Integer, SBS.ByteString m ())
takeLE n bs = foldlM (\(!a, bs') shift -> do
                        (x, bs'') <- take8 bs'
                        pure (a .|. (fromIntegral x `shiftL` shift), bs''))
                     (0, bs) (fmap (*8) [0..n-1])

printNumeric :: String -> Bool -> Word8 -> Proto.PrecScale -> SBS.ByteString IO ()
             -> IO (SBS.ByteString IO ())
printNumeric s True sz precScale d = do
  (realSz, d') <- take8 d
  if realSz == 0
    then do
      putStrLn (s ++ "(" ++ show sz ++ ", " ++ show precScale ++ "): (NULL)")
      pure d'
    else printNumeric s False realSz precScale d'
printNumeric s False sz precScale@(Proto.PrecScale p scale) d = do
  putStr (s ++ "(" ++ show sz ++ ", " ++ show precScale ++ "):")
  (sign, d') <- take8 d
  let intSz | p <=  9 = 4
            | p <= 19 = 8
            | p <= 28 = 12
            | otherwise = 16
  (num, d'') <- takeLE (fromIntegral sz - 1) d'
  let res = num % (10 ^ fromIntegral scale)

      res' :: Rational
      res' = if sign == 0 then negate res else res

  putStrLn (show res')

  pure d''

printColumn :: Proto.ColumnData -> SBS.ByteString IO () -> IO (SBS.ByteString IO ())
printColumn ty d =
  case Proto.cdBaseTypeInfo ty of
    Proto.VarcharType typeLen Proto.NationalChar len coll -> do
--      putStr ("NVARCHAR(" ++ show len ++ ") COLLATION " ++ show coll ++ ": ")
      (len, d') <- takeLength typeLen d

      if len == 0xFFFF
         then do
           putStrLn "(NULL)"
           pure d'
         else do
           let d'' = SBS.splitAt (fromIntegral len) d'
           byteData S.:> d''' <- SBS.toStrict d''
           T.putStrLn (TE.decodeUtf16LE byteData)

           pure d'''
    Proto.IntNType False 4 -> do
      (n, d') <-takeLE 4 d
--      putStrLn ("INT: " ++ show n)
      pure d'
    Proto.IntNType False bytes -> do
      (n, d') <- takeLE (fromIntegral bytes) d
--      putStrLn ("INT(" ++ show bytes ++ "): " ++ show n)
      pure d'
    Proto.IntNType True bytes -> do
      (realWidth, d') <- take8 d
      if realWidth == 0
         then do
           putStrLn ("INT(" ++ show bytes ++ "): (NULL)")
           pure d'
         else do
           (n, d'') <- takeLE (fromIntegral bytes) d'
           putStrLn ("INT(" ++ show bytes ++ "): " ++ show n)
           pure d''
    Proto.DecimalNType nullable sz precScale ->
        printNumeric "DECIMAL" nullable sz precScale d
    Proto.NumericNType nullable sz precScale ->
        printNumeric "NUMERIC" nullable sz precScale d
    _ -> fail ("Can't print data of type " ++ show ty)
