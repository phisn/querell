module Execution.Execute where

import Control.Monad (guard, when)
import Control.Monad qualified as Monad
import Control.Monad.Except
import Control.Monad.Except qualified as Except
import Control.Monad.IO.Class as MonadIO
import Control.Monad.Reader qualified as Reader
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Maybe (maybeToExceptT)
import Control.Monad.Writer qualified as Writer
import Data.Bits (Bits (xor))
import Data.Either (fromRight)
import Data.Either.Combinators (maybeToRight)
import Data.IORef qualified as IORef
import Data.Map qualified as Map
import Data.Text (Text)
import Data.Vector.Mutable as IOVector
import Execution
import Execution.Evaluate
import Plan (Plan)
import Plan qualified
import Streaming
import Streaming.Prelude qualified as S

{--
data BuilderWrapped where
  BuilderWrapped :: ColumnBuilder a -> BuilderWrapped

data BatchBuilder

execute :: forall m. (MonadExecutionContext m, MonadColumnFactory m, MonadError Text m, MonadIO m) => Plan -> Execution m ()
execute (Plan.Filter expr source) = do
  filterProcess (execute source) Map.empty 0
  where
    filterProcess :: Execution m () -> Map.Map Text BuilderWrapped -> Int -> Execution m ()
    filterProcess stream bufferBatch = do
      eitherItem <- lift $ S.next stream

      case eitherItem of
        Left () -> S.yield bufferBatch
        Right item -> filterProcessBatch bufferBatch item

    filterProcessBatch :: Batch -> (Batch, Execution m ()) -> Execution m ()
    filterProcessBatch bufferBatch (batch, stream) = do
      let incomingRows = batchRows batch
      maxRows <- lift executionBatchSize
      let rows = batchRows bufferBatch
      let rowsToWrite = min incomingRows $ maxRows - rows
      let rowsRemain = incomingRows - rowsToWrite
      let bufferBatchColumns = batchColumns batch

      Monad.forM_ (Map.toList $ batchColumns batch) $ \(key, value) -> do
        bufferColumn <- case Map.lookup key bufferBatchColumns of
          Just x -> return x
          Nothing -> error ""

        error ""

      error ""

{--
S.mapM filterBatch
where
  filterBatch :: Batch -> m Batch
  filterBatch batch = do
    let rows = batchRows batch

    (filterMaskCount, filterMask) <- evaluateFold batch expr 0 $
      \aggr result -> aggr + fromEnum result

    case filterMaskCount of
      0 -> batchMapColumnsM batch $ \x -> return x
      _ | rows == filterMaskCount -> return batch
      _ ->
        batchMapColumnsM batch $ \x -> do
          column <- filterColumn filterMaskCount rows x
          return $ ColumnWrapped column

  filterColumn :: Int -> Int -> Column Bool -> Column a -> m (Column b)
  filterColumn filterMaskCount n filterMask column = do
    Monad.forM_ [0 .. (n - 1)] $ \i -> do
      error ""
    error ""
    where
      filterPass = do
        x <- liftIO $ IOVector.read column indexRead
        liftIO $ IOVector.write column indexWrite x
        filterColumn (filterMaskCount, n, indexRead + 1, indexWrite + 1) filterMask column

      filterStop =
        filterColumn (filterMaskCount, n, indexRead + 1, indexWrite) filterMask column
        -}
execute _ = error ""

{--
  filter' :: Execution () -> Batch -> Execution ()
  filter' cursor buffer = takeBatch cursor $ \batch -> do
    let rows = rowCount batch

    f <- Except.liftEither $ evaluate batch expr

    let batch' = Map.map ((,Nothing) . Just) (columns batch)
    let buffer' = Map.map ((Nothing,) . Just) (columns buffer)

    let c = Map.toList $ Map.unionWith (\(a, _) (_, b) -> (a, b)) buffer' batch'

    Monad.forM_ [0 .. (rows - 1)] $ \i -> do
      b <- liftIO $ f i

      when b $ do
        return ()

    filter' cursor buffer

takeBatch :: Execution () -> (Batch -> Execution ()) -> Execution ()
takeBatch cursor f = do
  maybeBatch <- lift $ S.head_ cursor
  Monad.forM_ maybeBatch f

-- columnsBuffer :: Batch -> Execution ()
-- columnsBuffer columnBuffer name =
-- error ""

-- column <- Except.liftEither $ maybeToRight "" $ findColumn (buffer columnBuffer) name

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
--}
--}