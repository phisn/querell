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

evaluate :: Columns -> Plan.Expression a -> (Int -> IO a)
evaluate columns (Plan.Ref name)
  | Just (Column vector) <- Map.lookup name columns =
      case cast vector of
        Just vector' -> \i -> IOVector.unsafeRead vector' i
        Nothing -> error ""
  | otherwise = error ""
evaluate _ (Plan.Literal a) = \_ -> return a
evaluate columns (Plan.Compare l op r) = \i -> do
  l'' <- l' i
  r'' <- r' i
  return $ cmp l'' r''
  where
    l' = evaluate columns l
    r' = evaluate columns r

    makeCmp :: (Ord a) => Plan.CompareOp -> a -> a -> Bool
    makeCmp Plan.LE = (<)
    makeCmp Plan.LEQ = (<=)
    makeCmp Plan.EQ = (==)
    makeCmp Plan.NEQ = (/=)
    makeCmp Plan.GE = (>)
    makeCmp Plan.GEQ = (>=)

    cmp = makeCmp op
evaluate columns (Plan.Arithmetic l op r) = \i -> do
  l'' <- l' i
  r'' <- r' i
  return $ arit l'' r''
  where
    l' = evaluate columns l
    r' = evaluate columns r

    makeArit :: (Num a) => Plan.ArithmeticOp -> a -> a -> a
    makeArit Plan.Plus = (+)
    makeArit Plan.Minus = (-)
    makeArit Plan.Mul = (*)

    arit = makeArit op

mapColumn :: IOVector a -> (a -> b) -> IO (IOVector b)
mapColumn v f = do
  let len = IOVector.length v
  v'' <- IOVector.new len
  apply len v v''
  where
    apply 0 _ r = return r
    apply i v' v'' = do
      x <- IOVector.read v' i
      IOVector.unsafeWrite v'' (i - 1) (f x)
      apply (i - 1) v' v''
