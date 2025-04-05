module Execution.Data where

import Data.Map (Map)
import Data.Map qualified as Map
import Data.Maybe (fromJust)
import Data.Vector.Unboxed.Mutable (IOVector)
import Data.Vector.Unboxed.Mutable qualified as Vector
import Plan qualified as Plan

data RecordBatch a = RecordBatch Plan.Schema (Map String (IOVector a))

data Column a where
  ColumnInt :: IOVector Int -> Column Int
  ColumnString :: IOVector String -> Column String

class MapNumeric a where
  map :: a -> a
