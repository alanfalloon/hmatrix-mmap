{-# LANGUAGE ScopedTypeVariables #-}
module Data.Packed.Vector.MMap (
  unsafeMMapVector,
  hPutVector,
  writeVector
) where

import System.IO
import System.IO.MMap

import Foreign.ForeignPtr
import Foreign.Ptr
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

-- | Write out a vector verbatim into an open file handle.
hPutVector :: forall a. Storable a => Handle -> I.Vector a -> IO ()
hPutVector h v = withForeignPtr fp $ \p -> hPutBuf h (p `plusPtr` offset) sz
      where
        (fp, offset, n) = I.unsafeToForeignPtr v
        eltsize = sizeOf (undefined :: a)
        sz = n * eltsize

-- | Write the vector verbatim to a file.
writeVector :: forall a. Storable a => FilePath -> I.Vector a -> IO ()
writeVector fp v = withFile fp WriteMode $ \h -> hPutVector h v
