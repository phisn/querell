module Main (main) where

import Control.Monad.Except (MonadError (throwError))
import Criterion.Main
import Data.Coerce (coerce)
import Data.Constraint
import Data.Either (fromRight)
import Data.Int
import Data.Text qualified as T
import Data.Typeable ((:~:) (Refl))
import Data.Typeable qualified as Typ
import Data.Vector qualified as V
import Data.Vector.Unboxed qualified as VU
import Execution
import GHC.Exts
import Plan
import TemplateUnroll
import Unsafe.Coerce (unsafeCoerce)

class CapabilityNumber a where
  cadd :: a -> a -> a
  csub :: a -> a -> a
  cmul :: a -> a -> a

instance CapabilityNumber (VU.Vector Int32) where
  {-# INLINE cadd #-}
  cadd = VU.zipWith (+)
  {-# INLINE csub #-}
  csub = VU.zipWith (-)
  {-# INLINE cmul #-}
  cmul = VU.zipWith (*)

instance CapabilityNumber (VU.Vector Float) where
  {-# INLINE cadd #-}
  cadd = VU.zipWith (+)
  {-# INLINE csub #-}
  csub = VU.zipWith (-)
  {-# INLINE cmul #-}
  cmul = VU.zipWith (*)

data Expr' where
  Arithmetic' :: Expr' -> Plan.ArithOp -> Expr' -> Expr'
  Column' :: Int -> Expr'

data Column' where
  ColumnInt32 :: !(VU.Vector Int32) -> Column'
  ColumnFloat :: !(VU.Vector Float) -> Column'

data ExExpr' a where
  ExColumnFloat :: !(VU.Vector Float) -> ExExpr' (VU.Vector Float)
  ExColumnInt32 :: !(VU.Vector Int32) -> ExExpr' (VU.Vector Int32)
  ExArithmetic :: (CapabilityNumber a) => !(ExExpr' a) -> Plan.ArithOp -> !(ExExpr' a) -> ExExpr' a

data ExExprWithType where
  SomeExExpr :: ExType a -> !(ExExpr' a) -> ExExprWithType

data ExType a where
  ExInt32 :: ExType (VU.Vector Int32)
  ExFloat :: ExType (VU.Vector Float)

deriving instance Show (ExType a)

toExecutional :: (MonadError T.Text m) => V.Vector (VU.Vector Int32) -> Expr' -> m ExExprWithType
toExecutional cols = go
  where
    go (Column' i) =
      return . SomeExExpr ExInt32 $ ExColumnInt32 $ cols V.! i
    go (Arithmetic' l op r) = do
      SomeExExpr lt l' <- go l
      SomeExExpr rt r' <- go r

      case (lt, rt) of
        (ExInt32, ExInt32) -> return . SomeExExpr ExInt32 $ ExArithmetic l' op r'
        (ExFloat, ExFloat) -> return . SomeExExpr ExFloat $ ExArithmetic l' op r'
        _ -> throwError $ "Failed convert types " <> (T.pack $ show lt) <> " and " <> (T.pack $ show rt)

{--
conv cols (Arithmetic' l op r) = do
  SomeExExpr (l' :: ExExpr' a) <- conv cols l
  SomeExExpr (r' :: ExExpr' b) <- conv cols r

  case Typ.eqT @a @b of
    Just Typ.Refl -> error ""
    Nothing -> error ""
--}

data ExprToEx where
  ExprToEx :: (Typ.Typeable a) => !(ExExpr' a) -> ExprToEx

evaluate''' :: V.Vector (VU.Vector Int32) -> Expr' -> VU.Vector Int32
evaluate''' batch (Arithmetic' l o r) =
  let f =
        case o of
          Plan.Plus -> (+)
          Plan.Minus -> (-)
          Plan.Mul -> (*)
          _ -> error ""
   in VU.zipWith f (evaluate''' batch l) (evaluate''' batch r)
evaluate''' batch (Column' v) =
  let !x = batch V.! v
   in x

$( unroll
     4
     [d|
       evaluateUnrolled :: (CapabilityNumber a) => ExExpr' a -> a
       evaluateUnrolled (ExArithmetic l o r) =
         (f o) (evaluateUnrolled l) (evaluateUnrolled r)
         where
           f Plan.Plus = (cadd)
           f Plan.Minus = (csub)
           f Plan.Mul = (cmul)
           f _ = error ""
       evaluateUnrolled (ExColumnInt32 v) = v
       evaluateUnrolled (ExColumnFloat v) = v
       |]
 )

evaluateUnrolle :: (CapabilityNumber a) => ExExpr' a -> a
evaluateUnrolle (ExArithmetic l o r) =
  (f o) (evaluateUnrolled l) (evaluateUnrolled r)
  where
    f Plan.Plus = (cadd)
    f Plan.Minus = (csub)
    f Plan.Mul = (cmul)
    f _ = error ""
evaluateUnrolle (ExColumnInt32 v) = v
evaluateUnrolle (ExColumnFloat v) = v

$( unroll
     4
     [d|
       evaluateUnrolledAlt :: V.Vector Column' -> Expr' -> Column'
       evaluateUnrolledAlt batch (Arithmetic' l o r) =
         case ((evaluateUnrolledAlt batch l), (evaluateUnrolledAlt batch r)) of
           (ColumnInt32 l', ColumnInt32 r') -> ColumnInt32 $ VU.zipWith (f o) l' r'
           _ -> error ""
         where
           f Plan.Plus = (+)
           f Plan.Minus = (-)
           f Plan.Mul = (*)
           f _ = error ""
       evaluateUnrolledAlt batch (Column' v) = batch `V.unsafeIndex` v
       |]
 )

myTestRaw :: (VU.Vector Int32, VU.Vector Int32, VU.Vector Int32) -> Int32
myTestRaw (c0, c1, c2) = do
  VU.sum $ (VU.zipWith (*) (VU.zipWith (+) c0 c1) c2)

myTestBase :: (V.Vector (VU.Vector Int32), Expr') -> Int32
myTestBase (b, x) = do
  VU.sum $ evaluate''' b x

myTestUnrolledPre :: (V.Vector (VU.Vector Int32), Expr') -> Int32
myTestUnrolledPre (b, x) = do
  case toExecutional b x of
    Right (SomeExExpr ExInt32 x') -> VU.sum $ evaluateUnrolled x'
    _ -> error ""

myTestUnrolledPreComputed :: ExExpr' (VU.Vector Int32) -> Int32
myTestUnrolledPreComputed = VU.sum . evaluateUnrolled

myTestUnrolledPreComputedAlt :: (V.Vector Column', Expr') -> Int32
myTestUnrolledPreComputedAlt (b, x) =
  case evaluateUnrolledAlt b x of
    ColumnInt32 x' -> VU.sum x'
    _ -> error ""

main :: IO ()
main = do
  let size = 2 ^ 16

  let !c0 = VU.enumFromN (1 :: Int32) size
  let !c1 = VU.enumFromN (2 :: Int32) size
  let !c2 = VU.enumFromN (3 :: Int32) size

  let !c0' = VU.enumFromN (1 :: Float) size
  let !c1' = VU.enumFromN (2 :: Float) size
  let !c2' = VU.enumFromN (3 :: Float) size

  let (expr, t) = (Arithmetic' (Arithmetic' (Column' 0) Plus (Column' 1)) Mul (Column' 2), (c0, c1, c2))
  let (expr', t') = (Arithmetic' (Arithmetic' (Column' 1) Plus (Column' 2)) Mul (Column' 0), (c1, c2, c0))
  let (expr'', t'') = (Arithmetic' (Arithmetic' (Column' 2) Plus (Column' 0)) Mul (Column' 1), (c2', c0', c1'))

  let !k = (V.fromList [c0, c1, c2], expr)
  let (SomeExExpr ExInt32 !i) = fromRight (error "") $ toExecutional (V.fromList [c0, c1, c2]) expr

  let !k' = (V.fromList [c1, c2, c0], expr)
  let (SomeExExpr ExInt32 !i') = fromRight (error "") $ toExecutional (V.fromList [c1, c2, c0]) expr

  -- let !k'' = (V.fromList [c2, c0, c1], expr)
  -- let (!i'') = conv' (V.fromList [c2, c0, c1]) expr

  let !l = ColumnInt32 <$> V.fromList [c1, c2, c0]
  let !l' = ColumnInt32 <$> V.fromList [c1, c2, c0]

  putStrLn $ show $ myTestRaw t
  putStrLn $ show $ myTestBase k
  putStrLn $ show $ myTestUnrolledPre k

  defaultMain
    [ bench "Column Short benchmark" $ whnf myTestRaw t,
      bench "Column Base benchmark" $ whnf myTestBase k,
      bench "Column Pre-computed 1 Unrolled benchmark" $ whnf myTestUnrolledPre k,
      bench "Column Alt Unrolled benchmark" $ whnf myTestUnrolledPreComputedAlt (l, expr),
      bench "Column Pre-computed 2 Unrolled benchmark" $ whnf myTestUnrolledPreComputed i,
      bench "'Column Short benchmark" $ whnf myTestRaw t',
      bench "'Column Base benchmark" $ whnf myTestBase k',
      bench "'Column Pre-computed 1 Unrolled benchmark" $ whnf myTestUnrolledPre k',
      bench "'Column Alt Unrolled benchmark" $ whnf myTestUnrolledPreComputedAlt (l', expr),
      bench "'Column Pre-computed 2 Unrolled benchmark" $ whnf myTestUnrolledPreComputed i'
      {--      bench "''Column Short benchmark" $ whnf myTestRaw t'',
            bench "''Column Base benchmark" $ whnf myTestBase k'',
            bench "''Column Pre-computed 1 Unrolled benchmark" $ whnf myTestUnrolledPre k'',
            bench "''Column Alt Unrolled benchmark" $ whnf myTestUnrolledPreComputedAlt k'',
            bench "''Column Pre-computed 2 Unrolled benchmark" $ whnf myTestUnrolledPreComputed i''
            --}
    ]

-- -- --time-limit 1