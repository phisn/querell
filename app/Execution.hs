{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Execution where

import Control.Monad.Except qualified as Except
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader qualified as Reader
import Control.Monad.Writer qualified as Writer
import Data.Map (Map)
import Data.Map qualified as Map
import Execution.Data (RecordBatch)
import Execution.Data qualified as Data
import Plan (Plan)
import Plan qualified

newtype Datasource = Datasource
  { scan :: Maybe Plan.Expr -> Maybe [String] -> Execution ExecutionStep
  }

newtype ExecutionContext = ExecutionContext {sources :: Map String Datasource}

newtype Execution a = Execution
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

data ExecutionStep
  = Batch RecordBatch (Execution ExecutionStep)
  | End

source :: String -> Execution (Maybe Datasource)
source name = do
  sources' <- Reader.asks sources
  return $ Map.lookup name sources'

log :: String -> Execution ()
log message = Writer.tell [message]

fail :: String -> Execution a
fail = Except.throwError
