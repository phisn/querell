{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs #-}

module Execution where

import Control.Monad.Except
import Control.Monad.Except qualified as Except
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader qualified as Reader
import Control.Monad.Writer qualified as Writer
import Data.Kind (Type)
import Data.Map as Map
import Data.Text (Text)
import Data.Text qualified as T
import Data.Typeable
import Data.Vector.Mutable as IOVector
import Data.Vector.Primitive.Mutable (Prim)
import Plan (Plan)
import Plan qualified
import Streaming
import Streaming.Prelude qualified as S

data Column a where
  PrimColumn :: (Prim a) => IOVector a -> Column a

columnRow :: (MonadIO m) => Column a -> Int -> m a
columnRow (PrimColumn column) i = do
  liftIO $ IOVector.read column i

data ColumnWrapped where
  ColumnWrapped :: (Prim a) => Column a -> ColumnWrapped

data Batch where
  Columns ::
    { batchColumns :: Map Text ColumnWrapped,
      batchRows :: Int
    } ->
    Batch

batchColumn :: (MonadError Text m) => Batch -> Text -> m ColumnWrapped
batchColumn cs name =
  maybe failure return batchLookup
  where
    batchLookup = Map.lookup name (batchColumns cs)
    failure = throwError $ "Batch does not contain column " <> name

batchColumnTyped :: (Typeable a, MonadError Text m) => Batch -> Text -> m (Column a)
batchColumnTyped cs name = do
  column <- batchColumn cs name
  castWithError column

castWithError :: forall a b m. (Typeable a, Typeable b, Except.MonadError Text m) => a -> m b
castWithError x =
  case cast x of
    Just y -> return y
    Nothing ->
      throwError $
        "Type cast failed: cannot cast value of type "
          <> (T.pack . show) (typeOf x)
          <> " to target type "
          <> (T.pack . show) (typeRep (Proxy :: Proxy b))
          <> "."

class ColumnBuilder c a | c -> a

class (Monad m) => MonadBuildBatch m where
  type BuilderResult m :: Type -> Type
  buildBatchPrim :: forall a. (Prim a) => Int -> (Int -> ) -> m (Column a)

class (Monad m) => MonadDatasource m where
  datasourceScan :: Maybe (Plan.Expression Bool) -> Maybe [Text] -> Execution m [Batch]

class (Monad m) => MonadExecutionContext m where
  executionBatchSize :: m Int
  executionScan :: Text -> Maybe (Plan.Expression Bool) -> Maybe [Text] -> Execution m ()

type Execution a = Stream (Of Batch) a
