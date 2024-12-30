module Execution.Execute where

import Execution
import Execution.Evaluate
import Plan (Plan)
import Plan qualified

execute :: Plan -> IO Columns
execute = error ""