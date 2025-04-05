{-# LANGUAGE GADTs #-}

module Execution.Evaluate where

import Control.Monad qualified as Monad
import Control.Monad.Except
import Control.Monad.IO.Class
import Data.Either (fromRight)
import Data.Map as Map
import Data.Text
import Data.Typeable
import Data.Vector.Mutable as IOVector
import Execution
import Plan qualified

evaluate :: (MonadError Text m, MonadIO m) => Batch -> Plan.Expression a -> m (Column a)
evaluate batch expr = do
  let rows = batchRows batch
  f <- evaluateF batch expr
  result <- liftIO $ IOVector.new $ rows

  Monad.forM_ [0 .. (rows - 1)] $ \i -> do
    element <- f i
    liftIO $ IOVector.write result i element

  return result

evaluateF :: (MonadError Text m, MonadIO m) => Batch -> Plan.Expression a -> m (Int -> m a)
evaluateF columns (Plan.Ref name) = do
  column <- batchColumn columns name
  typed <- castWithError column
  return $ liftIO <$> IOVector.unsafeRead typed
evaluateF _ (Plan.Literal a) = return $ \_ -> return a
evaluateF columns (Plan.Compare l op r) = do
  l' <- evaluateF columns l
  r' <- evaluateF columns r
  return $ \i -> do
    l'' <- l' i
    r'' <- r' i
    return $ f l'' r''
  where
    f = makef op

    makef :: (Ord a) => Plan.CompareOp -> a -> a -> Bool
    makef Plan.LE = (<)
    makef Plan.LEQ = (<=)
    makef Plan.EQ = (==)
    makef Plan.NEQ = (/=)
    makef Plan.GE = (>)
    makef Plan.GEQ = (>=)
evaluateF columns (Plan.Arithmetic l op r) = do
  l' <- evaluateF columns l
  r' <- evaluateF columns r
  return $ \i -> do
    l'' <- l' i
    r'' <- r' i
    return $ f l'' r''
  where
    f = makef op

    makef :: (Num a) => Plan.ArithmeticOp -> a -> a -> a
    makef Plan.Plus = (+)
    makef Plan.Minus = (-)
    makef Plan.Mul = (*)
evaluateF columns (Plan.Logical l op r) = do
  l' <- evaluateF columns l
  r' <- evaluateF columns r
  return $ \i -> do
    l'' <- l' i
    r'' <- r' i
    return $ f l'' r''
  where
    f = makef op

    makef :: Plan.LogicalOp -> Bool -> Bool -> Bool
    makef Plan.And = (&&)
    makef Plan.Or = (||)
