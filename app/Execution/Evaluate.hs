{-# LANGUAGE GADTs #-}

module Execution.Evaluate where

import Control.Monad qualified as Monad
import Data.Either (fromRight)
import Data.Map as Map
import Data.Typeable
import Data.Vector.Mutable as IOVector
import Execution
import Plan qualified

evaluate :: Batch -> Plan.Expression a -> Either String (Int -> IO a)
evaluate columns (Plan.Ref name) =
  case findColumn columns name of
    Just (Column vector) -> IOVector.unsafeRead <$> castWithError vector
    Nothing -> Left $ "Unable to find column " ++ name
evaluate _ (Plan.Literal a) = return $ \_ -> return a
evaluate columns (Plan.Compare l op r) = do
  l' <- evaluate columns l
  r' <- evaluate columns r
  return $ \i -> do
    l'' <- l' i
    r'' <- r' i
    return $ cmp l'' r''
  where
    cmp = makeCmp op

    makeCmp :: (Ord a) => Plan.CompareOp -> a -> a -> Bool
    makeCmp Plan.LE = (<)
    makeCmp Plan.LEQ = (<=)
    makeCmp Plan.EQ = (==)
    makeCmp Plan.NEQ = (/=)
    makeCmp Plan.GE = (>)
    makeCmp Plan.GEQ = (>=)
evaluate columns (Plan.Arithmetic l op r) = do
  l' <- evaluate columns l
  r' <- evaluate columns r
  return $ \i -> do
    l'' <- l' i
    r'' <- r' i
    return $ arit l'' r''
  where
    arit = makeArit op

    makeArit :: (Num a) => Plan.ArithmeticOp -> a -> a -> a
    makeArit Plan.Plus = (+)
    makeArit Plan.Minus = (-)
    makeArit Plan.Mul = (*)

test :: Batch -> Plan.Expression Bool -> IO ()
test columns expr = do
  let f = fromRight (error "") $ evaluate columns expr

  let len = rowCount columns
  target <- IOVector.new len

  passed <-
    Monad.foldM
      ( \acc i -> do
          b <- f i
          IOVector.unsafeWrite target i b
          return $ acc + fromEnum b
      )
      (0 :: Int)
      [0 .. len - 1]

  return ()
