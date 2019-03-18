module Database.TDS.Proto.Errors where

import Data.Word

-- | Error code in @ERROR@ token
newtype SQLError = SQLError Word32 deriving Show

-- * Error classes

-- | ERROR @cls@ field
newtype ErrorClass = ErrorClass Word8 deriving Show

data Severity
    = Information
    | Fatal
      deriving Show

clsSeverity :: ErrorClass -> Severity
clsSeverity (ErrorClass cls)
    | cls <= 10 = Information
    | cls >  10 = Fatal


