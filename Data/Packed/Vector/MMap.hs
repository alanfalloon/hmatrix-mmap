{-# LANGUAGE ScopedTypeVariables #-}
module Data.Packed.Vector.MMap (
  unsafeMMapVector
) where

import System.IO.MMap
import Foreign.Storable

import qualified Data.Packed.Development as I
import qualified Data.Packed.Vector as I
import Data.Int

-- | Map a file into memory ('ReadOnly' mode) as an immutable vector.
unsafeMMapVector :: forall a. Storable a => FilePath -- ^ Path of the file to map
                                         -> Maybe (Int64, Int) -- ^ 'Nothing' to map entire file into memory, otherwise 'Just (fileOffset, elementCount)'
                                         -> IO (I.Vector a)
unsafeMMapVector path range = 
  do (foreignPtr, offset, size) <- mmapFileForeignPtr path ReadOnly $ 
        case range of
          Nothing -> Nothing
          Just (start, length) -> Just (start, length * sizeOf (undefined :: a))
     return $ I.unsafeFromForeignPtr foreignPtr offset (size `div` sizeOf (undefined :: a))
