module Plan.Type where

import Control.Monad (when)
import Data.Map (Map)
import Data.Map qualified as Map
import Plan

class ToType a where
  typeOf :: a -> Schema -> Either String Type

checkTypeOf :: ([Type] -> Bool) -> [Expr] -> String -> ([Type] -> Type) -> Schema -> Either String Type
checkTypeOf check expr err reduce s = do
  t <- mapM (`typeOf` s) expr
  when (check t) (Left err)
  pure $ reduce t

checkTypeOf'number :: [Expr] -> String -> ([Type] -> Type) -> Schema -> Either String Type
checkTypeOf'number = checkTypeOf (all (TInt ==))

checkTypeOf'same :: [Expr] -> String -> ([Type] -> Type) -> Schema -> Either String Type
checkTypeOf'same = checkTypeOf same
  where
    same :: [Type] -> Bool
    same [] = True
    same (x : xs) = all (x ==) xs

instance ToType BinaryExpr where
  typeOf e@(BinaryExpr a Plus b) = checkTypeOf'number [a, b] (show e) (const TInt)
  typeOf e@(BinaryExpr a Minus b) = checkTypeOf'number [a, b] (show e) (const TInt)
  typeOf e@(BinaryExpr a Multiply b) = checkTypeOf'number [a, b] (show e) (const TInt)
  typeOf e@(BinaryExpr a Equals b) = checkTypeOf'same [a, b] (show e) (const TBoolean)
  typeOf e@(BinaryExpr a Less b) = checkTypeOf'number [a, b] (show e) (const TBoolean)

instance ToType LiteralExpr where
  typeOf (Int _) _ = pure TInt
  typeOf (String _) _ = pure TString

instance ToType Expr where
  typeOf (Literal a) = typeOf a
  typeOf e@(Reference a) = \(Schema schema) -> do
    case Map.lookup a schema of
      Just (Field _ t) -> return t
      Nothing -> Left $ show e
  typeOf (Binary a) = typeOf a
  typeOf e@(Not a) = checkTypeOf (all (TBoolean ==)) [a] (show e) (const TBoolean)
