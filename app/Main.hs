module Main (main) where

import Criterion.Main
import Data.Int
import Data.Text qualified as T
import Data.Typeable qualified as Typ
import Data.Vector qualified as V
import Data.Vector.Unboxed qualified as VU
import Execution
import Execution.Evaluate
import Plan
import TemplateUnroll

data Expr' where
  Column' :: Int -> Expr'
  Arithmetic' :: Expr' -> Plan.ArithOp -> Expr' -> Expr'

class ColNum' a where
  cadd :: a -> a -> a
  csub :: a -> a -> a
  cmul :: a -> a -> a

instance ColNum' (VU.Vector Int32) where
  cadd = VU.zipWith (+)
  csub = VU.zipWith (-)
  cmul = VU.zipWith (*)

data ExExpr a where
  ExColumn :: (ColNum' a) => !a -> ExExpr a
  ExArithmetic :: (ColNum' a) => ExExpr a -> Plan.ArithOp -> ExExpr a -> ExExpr a

data ExprToEx where
  ExprToEx :: (Typ.Typeable a) => ExExpr a -> ExprToEx

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
       evaluateUnrolled :: ExExpr a -> a
       evaluateUnrolled (ExArithmetic l o r) = (f'' o) (evaluateUnrolled'' l) (evaluateUnrolled'' r)
         where
           f'' Plan.Plus = cadd
           f'' Plan.Minus = csub
           f'' Plan.Mul = cmul
           f'' _ = error ""
       evaluateUnrolled (ExColumn v) = v
       |]
 )

evaluateUnrolled'' :: ExExpr a -> a
evaluateUnrolled'' (ExArithmetic l o r) = (f'' o) (evaluateUnrolled'' l) (evaluateUnrolled'' r)
  where
    f'' Plan.Plus = cadd
    f'' Plan.Minus = csub
    f'' Plan.Mul = cmul
    f'' _ = error ""
evaluateUnrolled'' (ExColumn v) = v

myTestRaw :: (VU.Vector Int32, VU.Vector Int32, VU.Vector Int32) -> Int32
myTestRaw (c0, c1, c2) = do
  VU.sum $ (VU.zipWith (*) (VU.zipWith (+) c0 c1) c2)

myTestBase :: (V.Vector (VU.Vector Int32), Expr') -> Int32
myTestBase (b, x) = do
  VU.sum $ evaluate''' b x

myTestUnrolled :: (V.Vector (VU.Vector Int32), Expr') -> Int32
myTestUnrolled (b, x) = do
  case conv b x of
    ExprToEx expr ->
      case Typ.cast expr of
        Just (x' :: ExExpr (VU.Vector Int32)) -> VU.sum $ evaluateUnrolled x'
        Nothing -> error "Failed to convert"

main :: IO ()
main = do
  let size = 2 ^ 20

  let !c0 = VU.enumFromN (1 :: Int32) size
  let !c1 = VU.enumFromN (2 :: Int32) size
  let !c2 = VU.enumFromN (3 :: Int32) size

  let (expr, t) = (Arithmetic' (Arithmetic' (Column' 0) Plus (Column' 1)) Mul (Column' 2), (c0, c1, c2))

  let !k = (V.fromList [c0, c1, c2], expr)

  putStrLn $ show $ myTestRaw t
  putStrLn $ show $ myTestBase k
  putStrLn $ show $ myTestUnrolled k

  defaultMain
    [ bench "Column Short benchmark" $ whnf myTestRaw t,
      bench "Column Base benchmark" $ whnf myTestBase k,
      bench "Column Unrolled benchmark" $ whnf myTestUnrolled k
    ]
