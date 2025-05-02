{-# LANGUAGE RoleAnnotations #-}

module Main (main) where

import Criterion.Main
import Data.Coerce (coerce)
import Data.Int
import Data.Text qualified as T
import Data.Typeable qualified as Typ
import Data.Vector qualified as V
import Data.Vector.Unboxed qualified as VU
import Execution
import Execution.Evaluate
import Plan
import TemplateUnroll
import Unsafe.Coerce (unsafeCoerce)

data Expr' where
  Column' :: Int -> Expr'
  Arithmetic' :: Expr' -> Plan.ArithOp -> Expr' -> Expr'

class ColNum' a where
  cadd :: a -> a -> a
  csub :: a -> a -> a
  cmul :: a -> a -> a

instance ColNum' (VU.Vector Int32) where
  {-# INLINE cadd #-}
  cadd = VU.zipWith (+)
  {-# INLINE csub #-}
  csub = VU.zipWith (-)
  {-# INLINE cmul #-}
  cmul = VU.zipWith (*)

data ExExprAlt where
  ExColumnAlt :: !(VU.Vector Int32) -> ExExprAlt
  ExArithmeticAlt :: ExExprAlt -> Plan.ArithOp -> ExExprAlt -> ExExprAlt

data ExExpr a where
  ExColumn :: !(VU.Vector Int32) -> ExExpr (VU.Vector Int32)
  ExArithmetic :: ExExpr (VU.Vector Int32) -> Plan.ArithOp -> ExExpr (VU.Vector Int32) -> ExExpr (VU.Vector Int32)

data ExprToEx where
  ExprToEx :: !(ExExpr (VU.Vector Int32)) -> ExprToEx

conv :: V.Vector (VU.Vector Int32) -> Expr' -> ExprToEx
conv batch (Column' i) = ExprToEx $ ExColumn $ batch V.! i
conv batch (Arithmetic' l op r) =
  case (conv batch l, conv batch r) of
    (ExprToEx l', ExprToEx r') ->
      case Typ.cast (l', r') of
        Just ((l'', r'') :: (ExExpr (VU.Vector Int32), ExExpr (VU.Vector Int32))) ->
          let k :: ExExpr (VU.Vector Int32) = ExArithmetic l'' op r''
           in ExprToEx k
        Nothing -> error "Failed to Convert"

conv' :: V.Vector (VU.Vector Int32) -> Expr' -> ExprToEx
conv' batch (Column' i) =
  ExprToEx $ ExColumn $ batch V.! i
conv' batch (Arithmetic' l op r) =
  case (conv batch l, conv batch r) of
    (ExprToEx l', ExprToEx r') -> ExprToEx $ ExArithmetic (unsafeCoerce l') op (unsafeCoerce r')

-- conv batch (Column' i) = Column'' $ batch V.! i
-- conv batch (Arithmetic' l op r) = Arithmetic'' (conv batch l) op (conv batch r)

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
       evaluateUnrolled :: (ColNum' a) => ExExpr a -> a
       evaluateUnrolled (ExArithmetic l o r) =
         (f o) (evaluateUnrolled l) (evaluateUnrolled r)
         where
           f Plan.Plus = (cadd)
           f Plan.Minus = (csub)
           f Plan.Mul = (cmul)
           f _ = error ""
       evaluateUnrolled (ExColumn v) = v
       |]
 )

$( unroll
     4
     [d|
       evaluateUnrolledAlt :: V.Vector (VU.Vector Int32) -> Expr' -> VU.Vector Int32
       evaluateUnrolledAlt batch (Arithmetic' l o r) =
         VU.zipWith (f o) (evaluateUnrolledAlt batch l) (evaluateUnrolledAlt batch r)
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
  let ExprToEx (!x') = conv b x
  VU.sum . evaluateUnrolled $ x'

myTestUnrolledPreComputed :: ExExpr (VU.Vector Int32) -> Int32
myTestUnrolledPreComputed = VU.sum . evaluateUnrolled

myTestUnrolledPreComputedAlt :: (V.Vector (VU.Vector Int32), Expr') -> Int32
myTestUnrolledPreComputedAlt (b, x) = VU.sum $ evaluateUnrolledAlt b x

main :: IO ()
main = do
  let size = 2 ^ 13

  let !c0 = VU.enumFromN (1 :: Int32) size
  let !c1 = VU.enumFromN (2 :: Int32) size
  let !c2 = VU.enumFromN (3 :: Int32) size

  let (expr, t) = (Arithmetic' (Arithmetic' (Column' 0) Plus (Column' 1)) Mul (Column' 2), (c0, c1, c2))
  let (expr', t') = (Arithmetic' (Arithmetic' (Column' 1) Plus (Column' 2)) Mul (Column' 0), (c1, c2, c0))
  let (expr'', t'') = (Arithmetic' (Arithmetic' (Column' 2) Plus (Column' 0)) Mul (Column' 1), (c2, c0, c1))

  let !k = (V.fromList [c0, c1, c2], expr)
  let ExprToEx (!i) = conv (V.fromList [c0, c1, c2]) expr

  let !k' = (V.fromList [c1, c2, c0], expr)
  let ExprToEx (!i') = conv (V.fromList [c1, c2, c0]) expr

  let !k'' = (V.fromList [c2, c0, c1], expr)
  let ExprToEx (!i'') = conv (V.fromList [c2, c0, c1]) expr

  putStrLn $ show $ myTestRaw t
  putStrLn $ show $ myTestBase k

  defaultMain
    [ bench "Column Short benchmark" $ whnf myTestRaw t,
      bench "Column Base benchmark" $ whnf myTestBase k,
      bench "Column Pre-computed 1 Unrolled benchmark" $ whnf myTestUnrolledPre k,
      bench "Column Alt Unrolled benchmark" $ whnf myTestUnrolledPreComputedAlt k,
      bench "Column Pre-computed 2 Unrolled benchmark" $ whnf myTestUnrolledPreComputed i,
      bench "'Column Short benchmark" $ whnf myTestRaw t',
      bench "'Column Base benchmark" $ whnf myTestBase k',
      bench "'Column Pre-computed 1 Unrolled benchmark" $ whnf myTestUnrolledPre k',
      bench "'Column Alt Unrolled benchmark" $ whnf myTestUnrolledPreComputedAlt k',
      bench "'Column Pre-computed 2 Unrolled benchmark" $ whnf myTestUnrolledPreComputed i',
      bench "''Column Short benchmark" $ whnf myTestRaw t'',
      bench "''Column Base benchmark" $ whnf myTestBase k'',
      bench "''Column Pre-computed 1 Unrolled benchmark" $ whnf myTestUnrolledPre k'',
      bench "''Column Alt Unrolled benchmark" $ whnf myTestUnrolledPreComputedAlt k'',
      bench "''Column Pre-computed 2 Unrolled benchmark" $ whnf myTestUnrolledPreComputed i''
    ]
