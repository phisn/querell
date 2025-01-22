{-# LANGUAGE GADTs #-}

module Plan where

import Data.Typeable

data CompareOp = LE | LEQ | EQ | NEQ | GE | GEQ

data ArithmeticOp = Plus | Minus | Mul

data LogicalOp = And | Or

data Expression a where
  Arithmetic :: (Num a) => Expression a -> ArithmeticOp -> Expression a -> Expression a
  Compare :: (Ord a) => Expression a -> CompareOp -> Expression a -> Expression Bool
  Literal :: a -> Expression a
  Logical :: Expression Bool -> LogicalOp -> Expression Bool -> Expression Bool
  Ref :: (Typeable a) => String -> Expression a

data Plan where
  Filter :: Expression Bool -> Plan -> Plan
  Scan :: String -> Plan
