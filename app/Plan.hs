{-# LANGUAGE GADTs #-}

module Plan where

import Data.Typeable

data CompareOp = LE | LEQ | EQ | NEQ | GE | GEQ

data ArithmeticOp = Plus | Minus | Mul

data Expression a where
  Ref :: (Typeable a) => String -> Expression a
  Literal :: a -> Expression a
  Compare :: (Ord a) => Expression a -> CompareOp -> Expression a -> Expression Bool
  Arithmetic :: (Num a) => Expression a -> ArithmeticOp -> Expression a -> Expression a

data Plan where
  Scan :: String -> Plan
  Filter :: Expression Bool -> Plan -> Plan
