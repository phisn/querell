module Main (main) where

import Control.Monad (forM_)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import GHC.IORef (IORef (IORef))
import Streaming.Prelude (Of, Stream)
import Streaming.Prelude qualified as S

main :: IO ()
main = do
  let k = f

  (flip S.mapM_) k $ \x -> do
    e <- readIORef x
    putStrLn $ show e
    writeIORef x (e * 2)

  f'' <- (flip S.mapM_) k $ \x -> do
    e <- readIORef x
    putStrLn $ show e

  return ()

f :: Stream (Of (IORef Int)) IO ()
f = do
  liftIO (newIORef 1) >>= S.yield
  liftIO (newIORef 2) >>= S.yield
  liftIO (newIORef 3) >>= S.yield
  liftIO (newIORef 4) >>= S.yield
  liftIO (newIORef 5) >>= S.yield
