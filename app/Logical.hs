module Logical where

import Datasource qualified as DS

newtype Field = Field {name :: String} deriving (Show)

data BinaryExpr
  = Plus
  | Minus
  | Multiply
  | Equals
  | Less
  deriving (Show)

data UnaryExpr
  = Not
  | Negative
  deriving (Show)

data LiteralExpr
  = Int Int
  | String String
  deriving (Show)

data LogicalExpr
  = Literal LiteralExpr
  | Reference Field
  | Unary UnaryExpr LogicalExpr
  | Binary BinaryExpr LogicalExpr LogicalExpr
  deriving (Show)

data Plan
  = Scan DS.Datasource [Field]
  | Project [Field]
  | Filter LogicalExpr
  deriving (Show)
