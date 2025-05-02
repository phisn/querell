module TemplateUnroll (unroll) where

import Control.Monad (forM)
import Data.Generics (everywhere, mkT)
import Data.Maybe (listToMaybe)
import Language.Haskell.TH

isDef :: Dec -> Bool
isDef (FunD _ _) = True
isDef (ValD (VarP _) _ _) = True
isDef _ = False

getDefName :: Dec -> Maybe Name
getDefName (FunD n _) = Just n
getDefName (ValD (VarP n) _ _) = Just n
getDefName _ = Nothing

findSig :: Name -> [Dec] -> Q Dec
findSig n ds =
  maybe
    (fail $ "unroll: no type signature for “" ++ nameBase n)
    pure
    (listToMaybe [s | s@(SigD m _) <- ds, m == n])

unroll :: Int -> Q [Dec] -> Q [Dec]
unroll k qDecs = do
  decs <- qDecs

  originalDef <- case filter isDef decs of
    [d] -> pure d
    [] -> fail "unroll: no function/value definition found"
    _ -> fail "unroll: expected exactly one definition"

  let baseName = case getDefName originalDef of
        Just x -> x
        _ -> error "unroll: missing definition name"

  SigD _ origTy <- findSig baseName decs
  recName <- newName "rec"

  let substSelf :: Exp -> Exp
      substSelf = everywhere (mkT replace)
        where
          replace (VarE n) | n == baseName = VarE recName
          replace e = e

      tweakBody :: Body -> Body
      tweakBody (NormalB e) = NormalB (substSelf e)
      tweakBody (GuardedB gs) = GuardedB [(g, substSelf e) | (g, e) <- gs]

      addRec :: Dec -> Dec
      addRec (FunD _ cs) =
        FunD
          baseName
          [ Clause (VarP recName : pats) (tweakBody body) wh
          | Clause pats body wh <- cs
          ]
      addRec _ = error "unroll: only plain function definitions supported"

  let workerFun = addRec originalDef
      workerTy = ArrowT `AppT` origTy `AppT` origTy -- (a -> r) -> a -> r
  let cloneNames = [mkName (nameBase baseName ++ show i) | i <- [0 .. k - 1]]
      xName = mkName (nameBase baseName ++ "X")

      cloneSigs = [SigD n workerTy | n <- cloneNames ++ [xName]]
      renameFun new (FunD _ cs) = FunD new cs
      renameFun _ _ = error "impossible"
      cloneDefs = [renameFun n workerFun | n <- cloneNames ++ [xName]]

  let unrolledPrimeName = mkName (nameBase baseName ++ "'")
      unrolledName = mkName (nameBase baseName)

      unrolledPrimeDef =
        FunD
          unrolledPrimeName
          [Clause [] (NormalB (AppE (VarE xName) (VarE unrolledPrimeName))) []]
      unrolledPrimeSig = SigD unrolledPrimeName origTy

      chain = foldr AppE (VarE unrolledPrimeName) (map VarE cloneNames)
      unrolledDef = FunD unrolledName [Clause [] (NormalB chain) []]
      unrolledSig = SigD unrolledName origTy

  return $
    cloneSigs
      ++ cloneDefs
      ++ [ unrolledPrimeSig,
           unrolledPrimeDef,
           unrolledSig,
           unrolledDef
         ]
