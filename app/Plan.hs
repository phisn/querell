module Plan where

import Control.Monad.Except
import Control.Monad.Except qualified as Except
import Data.Map as Map
import Data.Text
import Data.Text (Text)
import Data.Text qualified as T
import Data.Typeable

data ArithmeticOp = Plus | Minus | Mul

data CompareOp = LE | LEQ | EQ | NEQ | GE | GEQ

data LogicalOp = And | Or

data Expression a where
  Arithmetic :: (Num a) => Expression a -> ArithmeticOp -> Expression a -> Expression a
  Compare :: (Ord a) => Expression a -> CompareOp -> Expression a -> Expression Bool
  Literal :: a -> Expression a
  Logical :: Expression Bool -> LogicalOp -> Expression Bool -> Expression Bool
  Ref :: (Typeable a) => Text -> Expression a

data Plan where
  Filter ::
    { schema :: Schema,
      filterExpr :: Expression Bool,
      child :: Plan
    } ->
    Plan
  Scan ::
    { schema :: Schema,
      column :: Text
    } ->
    Plan
  Coalesce ::
    { child :: Plan
    } ->
    Plan

test :: Plan -> ()
test x = error ""
  where
    test = x.filterExpr

type PlanTyped = (Schema, Plan)

data Schema where
  Schema :: Map Text Field -> Schema

data Field where
  Field ::
    { fieldIndex :: Int,
      fieldName :: Text,
      filedType :: ColumnType
    } ->
    Field

data ColumnType where
  CTBool :: ColumnType
  CTInt :: ColumnType
  CTNumber :: ColumnType
  CTString :: ColumnType
  deriving (Eq)

schemaField :: (MonadError Text m) => Schema -> Text -> m Field
schemaField (Schema schema) name =
  maybe failure return batchLookup
  where
    batchLookup = Map.lookup name schema
    failure = throwError $ "Batch does not contain column " <> name

schemaFields :: Schema -> [Field]
schemaFields (Schema schema) = Map.elems schema

schemaDerive :: Plan -> Schema
schemaDerive = error ""

schemaDeriveExpr :: Schema -> Expression a -> ColumnType
schemaDeriveExpr schema (Arithmetic l _ r)
  | tl == CTInt && tr == CTInt = CTInt
  | otherwise = CTNumber
  where
    tl = schemaDeriveExpr schema l
    tr = schemaDeriveExpr schema l
schemaDeriveExpr schema (Compare l _ r)
  | tl == CTInt && tr == CTInt = CTInt
  | otherwise = CTNumber
  where
    tl = schemaDeriveExpr schema l
    tr = schemaDeriveExpr schema l
