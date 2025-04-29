module Execution.Evaluate where

import Control.Monad qualified as Monad
import Control.Monad.Except
import Control.Monad.IO.Class
import Data.Either (fromRight)
import Data.Map as Map
import Data.Text qualified as T
import Data.Typeable
import Data.Typeable (eqT)
import Data.Vector qualified as Vector
import Data.Vector.Mutable as IOVector
import Data.Vector.Unboxed qualified as VectorU
import Execution
import GHC.IORef qualified as IORef
import GHC.Int
import Plan qualified

evaluate :: (MonadError T.Text m) => Batch -> Plan.Expr -> m Column
evaluate batch (Plan.Arithmetic l op r) = do
  (NumericColumn (l' :: DynamicColumn a')) <- asNumeric =<< evaluate batch l
  (NumericColumn (r' :: DynamicColumn a'')) <- asNumeric =<< evaluate batch r

  case eqT @a' @a'' of
    Just Refl -> return $ rebox $ binaryColumnOp (f op) l' r'
    Nothing -> throwError "Got invalid types!"
  where
    f Plan.Minus = divide
    f Plan.Mul = (*)
    f Plan.Plus = (+)
    f _ = error "Unknown arithmetic operator"
evaluate _ _ = error ""

{--
evaluate batch (Plan.Arithmetic l op r) = do
  (NumColumn (l' :: VectorU.Vector a')) <- evaluate batch l
  (NumColumn (r' :: VectorU.Vector a'')) <- evaluate batch r

  (Just Refl) <- return $ eqT @a' @a''

  return $ NumColumn $ VectorU.zipWith (f op) l' r'
  where
    f :: (Integral a) => Plan.ArithOp -> a -> a -> a
    f Plan.Div = div
    f Plan.Minus = (-)
    f Plan.Mul = (*)
    f Plan.Plus = (+)
evaluate batch (Plan.Column columnIndex) = do
  return $ batch.columns Vector.! columnIndex
evaluate batch (Plan.Literal (Plan.Int32 x)) =
  return $ Column $ x
--}

{--
evaluateAsF :: (MonadError Text m, MonadIO m, Typeable a) => Batch -> Plan.Expression -> m (Int -> a)
evaluateAsF columns (Plan.Arithmetic expr) = do
  l' <- (evaluateAsF columns expr.l) :: (Num x) => m (Int -> x)
  r' <- evaluateAsF columns expr.r
  return $ \i -> f (l' i) (r' i)
  where
    f = makef expr.arithmeticOp

    makef :: (Num a) => Plan.ArithmeticOp -> a -> a -> a
    makef Plan.Plus = (+)
    makef Plan.Minus = (-)
    makef Plan.Mul = (*)
evaluateAsF columns (Plan.Column name) = do
  column <- batchColumnTyped columns ""
  return $ columnRow column
--}

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