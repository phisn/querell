{-# LANGUAGE GADTs #-}

module Execution where

import Data.Map as Map
import Data.Typeable
import Data.Vector.Mutable as IOVector
import Plan (Plan)
import Plan qualified

data Column where
  Column :: (Typeable a) => IOVector a -> Column

type Columns = Map String Column
