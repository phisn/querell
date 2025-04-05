module Monad where

class (Monad m) => MonadLogger m where
  log :: String -> m ()
