module Plan.Schema where

import Control.Monad (when)
import Data.Map (Map)
import Data.Map qualified as Map
import Plan
import Plan.Type as Type

project :: [String] -> Schema -> Schema
project names (Schema fields) = Schema $ Map.filterWithKey (\x _ -> x `elem` names) fields

class ToSchema a where
  schemaOf :: a -> Either String Schema

instance ToSchema PlanScan where
  schemaOf (PlanScan (Datasource _ schema) (Just names)) = pure $ project names schema
  schemaOf (PlanScan (Datasource _ schema) Nothing) = pure schema

instance ToSchema PlanProject where
  schemaOf (PlanProject plan projections) = do
    schema@(Schema fields) <- schemaOf plan

    let (exprs, names) = unzip projections

    types <- mapM (`Type.typeOf` schema) exprs

    let listFields = uncurry Field <$> zip names types
    let fields' = Map.fromList $ zip names listFields

    return $ Schema $ Map.union fields fields'

instance ToSchema PlanFilter where
  schemaOf e@(PlanFilter plan expr) = do
    s <- schemaOf plan
    t <- typeOf expr s

    when (t == TBoolean) (Left $ show e)

    return s

instance ToSchema Plan where
  schemaOf (Filter a) = schemaOf a
  schemaOf (Project a) = schemaOf a
  schemaOf (Scan a) = schemaOf a
