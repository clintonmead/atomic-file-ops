{-# LANGUAGE NamedFieldPuns #-}

{-|
Often there's no clear way to preform atomic file system writes. The usual way
around this is to lock the file exclusively before writing. But file system locks
are generally advisory, so if an independent process which does not attempt to
gain a lock attempts to read the file, it may read an intermediate inconsistent state.

This package contains some functions for avoid this state of affairs.
It does this by creating a temp file, performing the writes to the temp file,
and then moving the temp file over the existing file. Generally in filesystems,
moves are atomic, so using this scheme a reading process should only see the old version
of the file or the new version of the file, and not an intermediate state.

One may be concerned if a file is being read as it is replaced that the start of
the old file is read and the end of the new file is read. However, this is not
likely to be the case, as when a file is opened, that same file is kept open and
readable even if it is say, deleted. As these functions do not modify the file,
but instead replace it, opening the file before it is atomically modified will
just result in old data being read, not an intermediate state.

The functions in this package reference classes from the package "io-string-like",
which means one can write 'String's, 'ByteString's or even 'Text's to the file
using the same function.
-}
module System.IO.AtomicFileOps (
  atomicReplaceFile, atomicModifyFile,
  AtomicTempOptions(tempFileDir, tempFileTemplate)
  )
where

import System.IO.StringLike.PutStr (CanPutStr, hPutStr)
import System.IO.StringLike.GetContents (CanGetContents, hGetContents)
import System.IO (openTempFile, hClose, Handle, withFile)
import System.Directory (copyPermissions, renameFile)
import System.FilePath (splitFileName)
import Data.Semigroup ((<>))
import System.IO.Error (catchIOError, ioError, isDoesNotExistError)
import Data.Maybe (fromMaybe)
import System.FileLock (withFileLock, SharedExclusive(Exclusive))
{-|
This data type is passed to 'atomicReplaceFile' and 'atomicModifyFile' to change
the behaviour of where the temp file is created.

The following will be called inside the atomic write functions:

> openTempFile tempFileDir tempFileTemplate

See the documentation for 'openTempFile' to see how these arguments are used.
-}
data AtomicTempOptions = AtomicTempOptions { tempFileDir :: FilePath, tempFileTemplate :: String }

defaultTempOptions :: FilePath -> AtomicTempOptions
defaultTempOptions filePath = AtomicTempOptions{tempFileDir, tempFileTemplate}
  where
    (tempFileDir, filename) = splitFileName filePath
    tempFileTemplate = filename <> ".tmp"

-- Internal function that just spits out the default temp options if you pass
-- it 'Nothing' but otherwise removes the 'Just'.
fromMaybeTempOptions :: FilePath -> Maybe AtomicTempOptions -> AtomicTempOptions
fromMaybeTempOptions filePath = fromMaybe (defaultTempOptions filePath)

{-|
> atomicReplaceFile options @file@ contents

atomically replaces @file@'s with @contents@.

@options@ should be @Just@ some 'AtomicTempOptions', which detail where the
temporary file is placed, or 'Nothing' for the default of creating a temp file
in the existing directory.

If the target file must exist.


As moving is atomic on filesystems generally, the target file should only ever
be in two states, either what it was like before it was written or what it was
like after.
-}
atomicReplaceFile :: CanPutStr contents => Maybe AtomicTempOptions -> FilePath -> contents -> IO ()
atomicReplaceFile maybeTempOptions fileToReplace contents = do
  let AtomicTempOptions{tempFileDir, tempFileTemplate} = fromMaybeTempOptions fileToReplace maybeTempOptions
  (tmpFileName, tmpHandle) <- openTempFile tempFileDir tempFileTemplate
  copyPermissions fileToReplace tmpFileName
  hPutStr tmpHandle contents
  hClose tmpHandle
  renameFile tmpFileName fileToReplace

{-|
Atomically modifies a file, by reading it and applying a function to it's contents.
This is different to 'atomicReplaceFile', which is best illistrated by an example.

Lets say a file contains a single integer, say "1". An IO statement is writen that
reads the file, adds one to the number, and then calls 'atomicReplaceFile' to
write it back out.

Running this should result the file being replaced with the contents "2".

Now lets say we run this from two different processes.

If the processes run sequentially, we will read "1", then write "2",
then read "2", then write "3".

But if the processes run overlapping, the first process may read "1", and before
it writes "2", the second process may read "1". The first process will then write
"2" but then also the second process will write "2".

We've clobbered a write, and the behavour is inconsistent based on timing.

This is where 'atomicModifyFile' comes in.

'atomicModifyFile' gets passed a function, and will lock the file, to ensure
two processes can not run on it at the same time.

If the two processes in the above case both used 'atomicModifyFile', then the
result would be "3" in call cases.

But it's worth mentioning that locking is advisory in many file systems. If you
don't consistently use 'atomicModifyFile' with all your writers you may get the
timing issues discussed previously. Indeed if you mix 'atomicReplaceFile' with
'atomicModifyFile' you may still get timing issues.

One may ask, if you're going to lock the files, why bother with temp files at all?

Well, you wouldn't need to if all your processes asked for locks. But 'atomicModifyFile'
achieves two things:

1. From writers that use 'atomicModifyFile': Writes occur sequentially
2. From all readers (including those who do not attempt to gain a lock):
   Will only ever see a consistent state.

Just using locks and not an "atomic style" temp file and replace does not achieve point 2.

The arguments to 'atomicModifyFile' are similar to 'atomicReplaceFile', except for
the third argument, which details how to replace the contents of the file.

This replacement function argument is quite complex. Lets say we have the following:

> f x = duringAction >> pure (afterAction, whatToWrite)
> atomicModifyFile options file f

The whole result of @f@ must be an IO action. Often this will be just @pure something@,
but in the above case I've included an actual IO action: @duringAction@.

The following sequences of events will occur:

1. The file will be exclusively locked.
2. The entire contents of the file will be read.
3. The IO @duringAction@ will be executed, as @f@ is run against the contents of the file.
4. If @whatToWrite@ is @Just contents@, @contents@ will be written to to a temp file and
   then moved over the target file.
   But if @whatToWrite@ is @Nothing@, do nothing.
5. The file will be unlocked. In theory this is unnecessary as we're now only
   locking the now dead file we just replaced. However that depends on whether
   the filesystem locks based on filename or filehandle. Anyway lets we play it safe
   and unlock, which we would need to do if we did nothing in part 4 anyway.
6. @afterAction@ will be executed as the IO return value of 'atomicModifyFile'.

So this function has a lot of flexibility, however, in many simple use cases
it will look something like this:

> atomicModifyFile options file (\contents -> pure (pure (), Just (f contents)))

Which just says apply @f@ to the contents of the contents of the file and do nothing else.

Like 'atomicReplaceFile', 'atomicModifyFile' will fail if the file
does not exist.
-}
atomicModifyFile :: (CanGetContents contents, CanPutStr contents)
  => Maybe AtomicTempOptions -> FilePath -> (contents -> IO (IO a, Maybe contents)) -> IO a
atomicModifyFile maybeTempOptions fileToReplace contentsFunction =
  withFileLock fileToReplace Exclusive actionDuringLock where
    actionDuringLock _ = do
      contents <- hGetContents fileToReplace
      (result, maybeNewContents) <- contentsFunction contents
      case maybeNewContents of
        Just newContents -> atomicReplaceFile maybeTempOptions fileToReplace newContents
        Nothing -> pure ()
      result
