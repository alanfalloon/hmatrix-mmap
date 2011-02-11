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
  unsafeLazyMMapVectors,

  -- * Writing 'Vector' to disk

  -- | These functions write the 'Vector' in a way suitable for
  -- reading back with 'unsafeMMapVector'.
  hPutVector,
  writeVector
) where

import Control.Monad (when)

import System.IO
import System.IO.MMap
import System.IO.Unsafe

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

-- | Map a file into memory as a lazy-list of equal-sized 'Vector',
-- even if they can't all fit in the address space at the same time.
--
-- > (numVectors,vectors) <- unsafeLazyMMapVectors filename Nothing vectorSize
--
-- Commonly, a data file will contain multiple vectors of equal length
-- (matrix). This function is convenient for those uses, but it plays
-- a more important role: supporting data-sets that cannot fit in the
-- address space of the current machine.
--
-- On 32-bit machines the address space is only 4GB, and it is
-- actually pretty easy to find data-sets that are too large to be
-- represented, even in virtual memory.
--
-- This function loads the data in chunks, and as long as you drop
-- your reference to the vectors as you consume the data, the old
-- chunks will be unmapped before mapping the next chunk.
--
-- The number of vectors in the list is returned because it's often
-- needed, yet calculating it using 'length' would demand the whole
-- list.
unsafeLazyMMapVectors :: forall a. Storable a => FilePath -- ^ Path of the file to map
                      -> Maybe (Int64, Int64)
                      -- ^ 'Nothing' to map entire file into memory,
                      -- otherwise @'Just' (fileOffset, totalElementCount)@
                      -> Int -- ^ The number of elements in each 'Vector'
                      -> IO (Int64,[I.Vector a]) -- ^ Return @(numberOfVectors,vectors)@
unsafeLazyMMapVectors path range vsize = do
  when (vecSize > maxChunkSize) vecTooBigError
  filesize <- withFile path ReadMode hFileSize
  let filesize' :: Int64
      filesize' = fI filesize
  imgs <- unsafeInterleaveIO $ unsafeLazyMMapVectors' filesize' path range vsize
  return (nimages range filesize', imgs)
      where
        nimages :: Maybe (Int64, Int64) -> Int64 -> Int64
        nimages Nothing fsz = fsz `div` imageSize
        nimages (Just (_,sz)) _ = sz `div` imageSize
        imageSize = fI vsize * eltSize
        eltSize = fI (sizeOf (undefined :: a))
        vecSize = fI vsize * eltSize
        vecTooBigError = fail "The requested vector size can't be mapped into memory"

unsafeLazyMMapVectors' :: forall a. Storable a => Int64
                       -> FilePath
                       -> Maybe (Int64, Int64)
                       -> Int
                       -> IO [I.Vector a]
unsafeLazyMMapVectors' fileSize
                       fileName
                       fileRange
                       numEltsPerVec
                           | mapSize < maxChunkSize = mmapAll
                           | otherwise = mmapChunks 0
    where
      mapSize, eltSize, vecSize, chunkSize, baseOffset :: Int64
      eltSize = fI $ sizeOf (undefined :: a)
      (baseOffset,mapSize) = case fileRange of
                               Just (off,nelts) -> (off,nelts*eltSize)
                               _ -> (0,fileSize)
      vecSize = fI numEltsPerVec * eltSize
      chunkSize = (maxChunkSize `div` vecSize) * vecSize

      fileRange' = do
        (offset, nelts) <- fileRange
        return (offset, fI nelts)

      splitVecs :: I.Vector a -> [I.Vector a]
      splitVecs bigVec = let nvecs = I.dim bigVec `div` numEltsPerVec
                         in I.takesV (replicate nvecs numEltsPerVec) bigVec

      mmapAll :: IO [I.Vector a]
      mmapAll = do
        allVecs <- unsafeMMapVector fileName fileRange'
        return $ splitVecs allVecs

      mmapChunks :: Int64 -> IO [I.Vector a]
      mmapChunks offs | remaining <= 0 = return []
                      | otherwise = do
        chunk <- unsafeMMapVector fileName mmapRange
        rest <- unsafeInterleaveIO $ mmapChunks (offs+chunkSize')
        return $ splitVecs chunk ++ rest
          where
            mmapRange = Just (baseOffset+offs,fI (chunkSize' `div` eltSize))
            remaining = mapSize-offs
            chunkSize' = min chunkSize remaining


-- Maximum size for chunks
maxChunkSize :: Int64
maxChunkSize = fI (maxBound `div` 4 :: Int)

-- Handy alias for 'fromIntegral'
fI :: (Integral a, Num b) => a -> b
fI = fromIntegral


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
