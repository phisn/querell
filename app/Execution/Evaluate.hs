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
import GHC.IORef qualified as IORef
import Plan qualified

{--
evaluateFold :: (MonadColumnFactory m, MonadError Text m, MonadIO m) => Batch -> Plan.Expression a -> aggr -> (aggr -> a -> aggr) -> m (aggr, Column a)
evaluateFold batch expr d folding = do
  let rows = batchRows batch
  builder <- builderNew rows

  f <- evaluateF batch expr

  let folding' aggr i = do
        let r = f i
        builderWrite builder i r
        return $ folding aggr r

  aggregated <- Monad.foldM folding' d [0 .. (rows - 1)]
  result <- builderFinish builder

  return (aggregated, result)

evaluate :: (MonadColumnFactory m, MonadError Text m, MonadIO m) => Batch -> Plan.Expression a -> m (Column a)
evaluate batch expr = do
  let rows = batchRows batch
  builder <- builderNew rows

  f <- evaluateF batch expr

  Monad.forM_ [0 .. (rows - 1)] $ \i -> do
    builderWrite builder i $ f i

  builderFinish builder

evaluateF :: (MonadError Text m, MonadIO m) => Batch -> Plan.Expression a -> m (Int -> a)
evaluateF columns (Plan.Ref name) = do
  column <- batchColumnTyped columns name
  return $ columnRow column
evaluateF _ (Plan.Literal a) = return $ \_ -> a
evaluateF columns (Plan.Compare l op r) = do
  l' <- evaluateF columns l
  r' <- evaluateF columns r
  return $ \i -> f (l' i) (r' i)
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
  return $ \i -> f (l' i) (r' i)
  where
    f = makef op

    makef :: (Num a) => Plan.ArithmeticOp -> a -> a -> a
    makef Plan.Plus = (+)
    makef Plan.Minus = (-)
    makef Plan.Mul = (*)
evaluateF columns (Plan.Logical l op r) = do
  l' <- evaluateF columns l
  r' <- evaluateF columns r
  return $ \i -> f (l' i) (r' i)
  where
    f = makef op

    makef :: Plan.LogicalOp -> Bool -> Bool -> Bool
    makef Plan.And = (&&)
    makef Plan.Or = (||)
--}