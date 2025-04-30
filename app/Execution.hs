module Execution where

import Control.Monad (when)
import Control.Monad.Except
import Control.Monad.Except qualified as Except
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader qualified as Reader
import Control.Monad.Writer qualified as Writer
import Data.Bit (Bit)
import Data.IORef qualified as IORef
import Data.Kind (Type)
import Data.Map as Map
import Data.Text (Text)
import Data.Text qualified as T
import Data.Typeable
import Data.Vector qualified as Vector
import Data.Vector.Unboxed (Unbox)
import Data.Vector.Unboxed qualified as VectorU
import GHC.Int
import Plan (Plan)
import Plan qualified
import Streaming
import Streaming.Prelude qualified as S

{--
data ArithmeticProps a' a'' = ArithmeticProps
  { l :: ExecutionExpr a',
    arithmeticOp :: Plan.ArithmeticOp,
    r :: ExecutionExpr a''
  }

data ExecutionExpr a where
  Arithmetic :: (Num a, Num a', Num a'') => ArithmeticProps a' a'' -> ExecutionExpr a
  Column :: Int -> ExecutionExpr a

exprToExecution :: Plan.Expression -> ExecutionExpr a
exprToExecution e@(Plan.Arithmetic a)
  | t == Plan.CTInt = ir
  where
    ir = Arithmetic $ ArithmeticProps {l = l, arithmeticOp = a.arithmeticOp, r = r}

    t = exprType e
    l = exprToExecution a.l
    r = exprToExecution a.r
exprToExecution (Plan.Column index) = error ""
exprToExecution _ = error ""

exprType :: Plan.Expression -> Plan.ColumnType
exprType = error ""
--}

data Column where
  BoolColumn :: VectorU.Vector Bit -> Column
  FloatColumn :: VectorU.Vector Float -> Column
  Int32Column :: VectorU.Vector Int32 -> Column
  StringColumn :: VectorU.Vector Text -> Column
  ScalarColumn :: Plan.Value -> Column

class (Unbox a, Typeable a) => ColBase a where
  rebox :: DynamicColumn a -> Column

instance ColBase Bit where
  rebox (NormalDynamic x) = BoolColumn x
  rebox (ScalarDynamic x) = ScalarColumn (Plan.Bool x)

instance ColBase Float where
  rebox (NormalDynamic x) = FloatColumn x
  rebox (ScalarDynamic x) = ScalarColumn (Plan.Float x)

instance ColBase Int32 where
  rebox (NormalDynamic x) = Int32Column x
  rebox (ScalarDynamic x) = ScalarColumn (Plan.Int32 x)

class (Num a, ColBase a) => ColNum a where
  divide :: a -> a -> a

instance ColNum Float where
  divide = (/)

instance ColNum Int32 where
  divide = div

data NumericColumn where
  NumericColumn :: (ColNum a) => DynamicColumn a -> NumericColumn

-- | Fail if the column is not numeric.
asNumeric :: (MonadError T.Text m) => Column -> m NumericColumn
asNumeric (FloatColumn v) = return $ NumericColumn $ NormalDynamic v
asNumeric (Int32Column v) = return $ NumericColumn $ NormalDynamic v
asNumeric (ScalarColumn (Plan.Float x)) = return $ NumericColumn $ ScalarDynamic $ x
asNumeric (ScalarColumn (Plan.Int32 x)) = return $ NumericColumn $ ScalarDynamic $ x
asNumeric _ = throwError "Got non-numeric column"

asBoolean :: (MonadError T.Text m) => Column -> m (DynamicColumn Bit)
asBoolean (BoolColumn v) = return $ NormalDynamic $ v
asBoolean (ScalarColumn (Plan.Bool x)) = return $ ScalarDynamic $ x
asBoolean _ = throwError "Got non-boolean column"

data DynamicColumn a where
  NormalDynamic :: (ColBase a) => !(VectorU.Vector a) -> DynamicColumn a
  ScalarDynamic :: (ColBase a) => !a -> DynamicColumn a

binaryColumnOp :: (a -> a -> a) -> DynamicColumn a -> DynamicColumn a -> DynamicColumn a
binaryColumnOp f (NormalDynamic l) (NormalDynamic r) = NormalDynamic $ VectorU.zipWith f l r
binaryColumnOp f (ScalarDynamic l) (NormalDynamic r) = NormalDynamic $ VectorU.map (f l) r
binaryColumnOp f (NormalDynamic l) (ScalarDynamic r) = NormalDynamic $ VectorU.map (f r) l
binaryColumnOp f (ScalarDynamic l) (ScalarDynamic r) = ScalarDynamic $ f l r

data Batch where
  Batch ::
    { columns :: Vector.Vector Column,
      rows :: Int,
      schema :: Plan.Schema
    } ->
    Batch

{--
columnRow :: Column a -> Int -> a
columnRow (PrimColumn column) i = column PVector.! i

data ColumnBuilder a where
  PrimColumnBuilder :: (PIOVector.Prim a) => PIOVector.IOVector a -> ColumnBuilder a

builderWrite :: (MonadIO m) => ColumnBuilder a -> Int -> a -> m ()
builderWrite (PrimColumnBuilder vector) index item = do
  liftIO $ PIOVector.unsafeWrite vector index item

builderFinish :: (MonadIO m) => ColumnBuilder a -> m (Column a)
builderFinish (PrimColumnBuilder vector) = do
  vector' <- liftIO $ PVector.unsafeFreeze vector
  return $ PrimColumn vector'
--}

{--
batchMapColumnsM :: (Monad m) => Batch -> (ColumnWrapped -> m ColumnWrapped) -> m Batch
batchMapColumnsM (Batch {batchColumns, batchRows}) f = do
  batchColumns' <- mapM f batchColumns
  return $ Batch {batchColumns = batchColumns', batchRows}

batchColumn :: (MonadError Text m) => Batch -> Text -> m ColumnWrapped
batchColumn cs name =
  maybe failure return batchLookup
  where
    batchLookup = Map.lookup name (batchColumns cs)
    failure = throwError $ "Batch does not contain column " <> name

batchColumnTyped :: forall a m. (Typeable a, MonadError Text m) => Batch -> Text -> m (Column a)
batchColumnTyped cs name = do
  (ColumnWrapped column) <- batchColumn cs name
  maybe (castFailure column) return $ gcast column
  where
    castFailure :: forall a'. (Typeable a') => Column a' -> m (Column a)
    castFailure from =
      throwError $
        "Type cast failed: cannot cast value of type "
          <> (T.pack . show) (typeRep (Proxy :: Proxy a'))
          <> " to target type "
          <> (T.pack . show) (typeRep (Proxy :: Proxy a))
          <> "."
--}

{--
class (Monad m) => MonadColumnFactory m where
  builderNew :: Int -> m (ColumnBuilder a)

class (Monad m) => MonadDatasource m where
  datasourceScan :: Maybe (Plan.Expression Bool) -> Maybe [Text] -> Execution m [Batch]

class (Monad m) => MonadExecutionContext m where
  executionBatchSize :: m Int
  executionScan :: Text -> Maybe (Plan.Expression Bool) -> Maybe [Text] -> Execution m ()

type Execution a = Stream (Of Batch) a
--}