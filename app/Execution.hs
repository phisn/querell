{-# LANGUAGE GADTs #-}

module Execution where

import Control.Monad.Except qualified as Except
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader qualified as Reader
import Control.Monad.Writer qualified as Writer
import Data.Map as Map
import Data.Typeable
import Data.Vector.Mutable as IOVector
import Plan (Plan)
import Plan qualified
import Streaming
import Streaming.Prelude qualified as S

data Column where
  Column :: (Typeable a) => IOVector a -> Column

data Batch = Columns
  { rowCount :: Int,
    columns :: Map String Column
  }

findColumn :: Batch -> String -> Maybe Column
findColumn cs name = Map.lookup name (columns cs)

data Datasource where
  Datasource ::
    { scan :: Maybe (Plan.Expression Bool) -> Maybe [String] -> Execution [Batch]
    } ->
    Datasource

data ExecutionContext = ExecutionContext {batchSize :: Int, sources :: Map String Datasource}

newtype Inner a = Execution
  { runExecution :: Reader.ReaderT ExecutionContext (Writer.WriterT [String] (Except.ExceptT String IO)) a
  }
  deriving
    ( Applicative,
      Except.MonadError String,
      Functor,
      Monad,
      MonadIO,
      Reader.MonadReader ExecutionContext,
      Writer.MonadWriter [String]
    )

type Execution a = Stream (Of Batch) Inner a

castWithError :: forall a b. (Typeable a, Typeable b) => a -> Either String b
castWithError x =
  case cast x of
    Just y -> Right y
    Nothing ->
      Left $
        "Type cast failed: cannot cast value of type "
          ++ show (typeOf x)
          ++ " to target type "
          ++ show (typeRep (Proxy :: Proxy b))
          ++ "."
