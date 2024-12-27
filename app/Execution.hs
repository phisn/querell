{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Execution where

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Class (MonadTrans (lift))
import Control.Monad.Trans.Except (ExceptT)
import Control.Monad.Trans.Reader (ReaderT)
import Control.Monad.Trans.Writer (WriterT)
import Execution.Data (RecordBatch)
import Execution.Data qualified as Data
import Plan (Plan)
import Plan qualified

data Datasource = IAB

data DatasourceRegistry = AB

data ExecutionContext = ExecutionContext {}

newtype Execution a = Execution
  { runExecution :: ReaderT ExecutionContext (WriterT [String] IO) a
  }
  deriving (Functor, Applicative, Monad, MonadIO)

data ExecutionStep
  = Batch RecordBatch (Execution ExecutionStep)
  | End
  | Error String
