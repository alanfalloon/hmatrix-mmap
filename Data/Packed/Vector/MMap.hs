{-# LANGUAGE ScopedTypeVariables #-}

-- | Functions to represent a 'Vector' on disk in efficient, if
-- unportable, ways.
--
-- This module uses memory-mapping, a feature of all modern
-- operating-systems, to mirror the disk contents in memory. There are
-- quite a few advantages to memory-mapping files instead of reading
-- the files traditionally:
--
--  * Speed: memory-mapping is often much faster than traditional
--    reading.
--
--  * Memory efficiency: Memory-mapped files are loaded into RAM
--    on-demand, and easily swapped out. The upside is that the
--    program can work with data-sets larger than the available RAM,
--    as long as they are accessed carefully.
--
-- The caveat to using memory-mapping is that it makes the files
-- specific to the current architecture because of the endianness of
-- the data. For more information, see the description in
-- "System.IO.MMap"
--
-- If you wish to write the contents in a portable fashion, either use
-- the ASCII load and save functions in "Numeric.Container", or use
-- the binary serialization in "Data.Binary".

module Data.Packed.Vector.MMap (
  -- * Memory-mapping 'Vector' from disk
  unsafeMMapVector,

  -- * Writing 'Vector' to disk

  -- | These functions write the 'Vector' in a way suitable for
  -- reading back with 'unsafeMMapVector'.
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

---------------------------
-- Memory-Mapping 'Vector' from disk

-- | Map a file into memory (read-only) as a 'Vector'.
--
-- It is considered unsafe because changes to the underlying file may
-- (or may not) be reflected in the 'Vector', which breaks referential
-- transparency.
unsafeMMapVector :: forall a. Storable a => FilePath -- ^ Path of the file to map
                                         -> Maybe (Int64, Int) -- ^ 'Nothing' to map entire file into memory, otherwise 'Just (fileOffset, elementCount)'
                                         -> IO (I.Vector a)
unsafeMMapVector path range = 
  do (foreignPtr, offset, size) <- mmapFileForeignPtr path ReadOnly $ 
        case range of
          Nothing -> Nothing
          Just (start, length) -> Just (start, length * sizeOf (undefined :: a))
     return $ I.unsafeFromForeignPtr foreignPtr offset (size `div` sizeOf (undefined :: a))

---------------------------
-- Writing 'Vector' to disk

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
