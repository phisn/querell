{-# LANGUAGE GADTs #-}

module V2 where

import Data.Map (Map)
import Data.Map as Map
import Data.Typeable
import Data.Vector.Mutable (IOVector)
import Data.Vector.Mutable as IOVector

data Expression a where
  ERef :: (Typeable a) => String -> Expression a
  ECompare :: Expression a -> Expression a -> Expression Bool

data Plan where
  PScan :: String -> Plan
  PFilter :: Expression Bool -> Plan -> Plan

data Column where
  Column :: (Typeable a) => String -> IOVector a -> Column

type Columns = Map String Column

evaluate :: Columns -> Expression a -> IO (IOVector a)
evaluate columns (ERef name)
  | Map.null columns = error ""
  | Just (Column _ vec) <- Map.lookup name columns = do
      case cast vec of
        Just vec' -> return vec'
        Nothing -> error ""
  | otherwise = error ""
evaluate columns (ECompare l r) = do
  l' <- evaluate columns l
  r' <- evaluate columns r

  cmp l' r'
  where
    cmp :: IOVector a -> IOVector a -> IO (IOVector b)
    cmp = error ""

myFilter :: Columns -> Expression Bool -> IO ([Column])
myFilter columns expr = do
  expr' <- evaluate columns expr

  error ""

execute :: Plan -> IO (IOVector a)
execute (PScan field) = error ""
execute (PFilter filter' plan) = do
  r <- execute plan
  f <- evaluate Map.empty filter'

  return r
