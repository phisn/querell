module Plan where

import Data.Map (Map)

data Field = Field String Type deriving (Show)

newtype Schema = Schema (Map String Field) deriving (Show)

data Type = TInt | TString | TBoolean deriving (Show, Eq)

data BinarySymbol
  = Plus
  | Minus
  | Multiply
  | Equals
  | Less
  deriving (Show)

data BinaryExpr
  = BinaryExpr Expr BinarySymbol Expr
  deriving (Show)

data LiteralExpr
  = Int Int
  | String String
  deriving (Show)

data Expr
  = Binary BinaryExpr
  | Literal LiteralExpr
  | Not Expr
  | Reference String
  deriving (Show)

data Datasource = Datasource String Schema
  deriving (Show)

data PlanFilter = PlanFilter Plan Expr
  deriving (Show)

data PlanScan = PlanScan Datasource (Maybe [String])
  deriving (Show)

data PlanProject = PlanProject Plan [(Expr, String)]
  deriving (Show)

data Plan
  = Scan PlanScan
  | Project PlanProject
  | Filter PlanFilter
  deriving (Show)