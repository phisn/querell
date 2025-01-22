module Execution.Execute where

import Control.Monad (guard, when)
import Control.Monad qualified as Monad
import Control.Monad.Except qualified as Except
import Control.Monad.IO.Class as MonadIO
import Control.Monad.Reader qualified as Reader
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe (maybeToExceptT)
import Control.Monad.Writer qualified as Writer
import Data.Either (fromRight)
import Data.Either.Combinators (maybeToRight)
import Data.Map qualified as Map
import Data.Vector.Mutable as IOVector
import Execution
import Execution.Evaluate
import Plan (Plan)
import Plan qualified
import Streaming
import Streaming.Prelude qualified as S

execute :: Plan -> Execution ()
execute (Plan.Filter expr source) = do
  targetSize <- batchSize <$> Reader.ask

  error ""
  where
    filter' :: Execution () -> Batch -> Execution ()
    filter' cursor buffer = takeBatch cursor $ \batch -> do
      let rows = rowCount batch

      f <- Except.liftEither $ evaluate batch expr

      let batch' = Map.map ((,Nothing) . Just) (columns batch)
      let buffer' = Map.map ((Nothing,) . Just) (columns buffer)

      let c = Map.toList $ Map.unionWith (\(a, _) (_, b) -> (a, b)) buffer' batch'

      let insert 

      Monad.forM_ [0 .. (rows - 1)] $ \i -> do
        b <- liftIO $ f i

        when b $ do
          return ()

      filter' cursor buffer

takeBatch :: Execution () -> (Batch -> Execution ()) -> Execution ()
takeBatch cursor f = do
  maybeBatch <- lift $ S.head_ cursor
  Monad.forM_ maybeBatch f

columnsBuffer :: Batch -> Execution ()
columnsBuffer columnBuffer name = do
  column <- Except.liftEither $ maybeToRight "" $ findColumn (buffer columnBuffer) name
  error ""

myOperator :: Stream (Of Batch) IO () -> Plan.Expression Bool -> Stream (Of Batch) IO ()
myOperator input expr = (`S.mapM` input) $ \x -> do
  let f = fromRight (error "") $ evaluate x expr

  let len = rowCount x
  target <- IOVector.new len

  passed <-
    Monad.foldM
      ( \acc i -> do
          b <- f i
          IOVector.unsafeWrite target i b
          return $ acc + fromEnum b
      )
      (0 :: Int)
      [0 .. len - 1]

  return x

myOperator2 :: Stream (Of Batch) IO () -> Plan.Expression Bool -> Stream (Of Batch) IO ()
myOperator2 input expr = do
  x <- lift $ S.head_ input

  S.yield $ error ""

  return ()

  S.yield $ error ""
