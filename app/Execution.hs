module Execution where

import Control.Monad (when)
import Control.Monad.Except
import Control.Monad.Except qualified as Except
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader qualified as Reader
import Control.Monad.Writer qualified as Writer
import Data.IORef qualified as IORef
import Data.Kind (Type)
import Data.Map as Map
import Data.Text (Text)
import Data.Text qualified as T
import Data.Typeable
import Data.Vector qualified as Vector
import Data.Vector.Mutable qualified as IOVector
import Data.Vector.Primitive qualified as PVector
import Data.Vector.Primitive.Mutable qualified as PIOVector
import Plan (Plan)
import Plan qualified
import Streaming
import Streaming.Prelude qualified as S

{--
data Column a where
  PrimColumn :: (PVector.Prim a) => PVector.Vector a -> Column a

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

data ColumnWrapped where
  ColumnWrapped :: (Typeable a) => Column a -> ColumnWrapped

data Batch where
  Batch ::
    { batchColumns :: Vector.Vector ColumnWrapped,
      batchRows :: Int
    } ->
    Batch

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

class (Monad m) => MonadColumnFactory m where
  builderNew :: Int -> m (ColumnBuilder a)

class (Monad m) => MonadDatasource m where
  datasourceScan :: Maybe (Plan.Expression Bool) -> Maybe [Text] -> Execution m [Batch]

class (Monad m) => MonadExecutionContext m where
  executionBatchSize :: m Int
  executionScan :: Text -> Maybe (Plan.Expression Bool) -> Maybe [Text] -> Execution m ()

type Execution a = Stream (Of Batch) a
--}