module Execution.Execute where

import Execution
import Execution.Data (RecordBatch)
import Execution.Data qualified as Data
import Plan (Plan)
import Plan qualified

class Executable a where
  execute :: a -> Execution ExecutionStep

instance Executable Plan.PlanScan where
  execute (Plan.PlanScan (Plan.Datasource name schema) fields) = do
    return End
