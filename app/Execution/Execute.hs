module Execution.Execute where

import Control.Monad (when)
import Data.Maybe (isNothing)
import Execution
import Execution.Data (RecordBatch)
import Execution.Data qualified as Data
import Plan (Plan)
import Plan qualified

class Executable a where
  execute :: a -> Execution ExecutionStep

instance Executable Plan.PlanFilter where
  execute e@(Plan.PlanFilter child expr) = do
    process $ execute child
    where
      process :: Execution ExecutionStep -> Execution ExecutionStep
      process x = do
        result <- x

        case result of
          Execution.Batch batch cont -> return $ Execution.Batch batch $ process cont
          Execution.End -> return Execution.End

instance Executable Plan.PlanScan where
  execute e@(Plan.PlanScan (Plan.Datasource name _) fields) = do
    s <- Execution.source name

    case s of
      Nothing -> Execution.fail $ show e
      Just s' -> do
        Execution.log $ "Scanning " ++ name
        Execution.scan s' Nothing fields

instance Executable Plan
