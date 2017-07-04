/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log;

import java.io.File;
import java.nio.ByteBuffer;

import kafka.utils.CoreUtils.inLock;
import kafka.common.InvalidOffsetException;

/**
 * An index that maps offsets to physical file locations for a particular log segment. This index may be sparse:
 * that is it may not hold an entry for all messages in the log.
 * 
 * The index is stored in a file that is pre-allocated to hold a fixed maximum number of 8-byte entries.
 * 
 * The index supports lookups against a memory-map of this file. These lookups are done using a simple binary search variant
 * to locate the offset/location pair for the greatest offset less than or equal to the target offset.
 * 
 * Index files can be opened in two either ways as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an 
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 * 
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 * 
 * The file format is a series of entries. The physical format is a 4 byte "relative" offset and a 4 byte file location for the 
 * message with that offset. The offset stored is relative to the base offset of the index file. So, for example,
 * if the base offset was 50, then the offset 55 would be stored as 5. Using relative offsets in this way let's us use
 * only 4 bytes for the offset.
 * 
 * The frequency of entries is up to the user of this class.
 * 
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal 
 * storage format.
 */
class OffsetIndex(File file, Long baseOffset, Integer maxIndexSize = -1, Boolean writable = true);
    extends AbstractIndex<Long, Int>(file, baseOffset, maxIndexSize, writable) {

  override public void  entrySize = 8;
  ;
  /* the last offset in the index */
  private<this> var _lastOffset = lastEntry.offset;
  ;
  debug("Loaded index file %s with maxEntries = %d, maxIndexSize = %d, entries = %d, lastOffset = %d, file position = %d";
    .format(file.getAbsolutePath, maxEntries, maxIndexSize, _entries, _lastOffset, mmap.position))

  /**
   * The last entry in the index
   */
  private public void  OffsetPosition lastEntry = {
    inLock(lock) {
      _entries match {
        case 0 => OffsetPosition(baseOffset, 0);
        case s => parseEntry(mmap, s - 1).asInstanceOf<OffsetPosition>;
      }
    }
  }

  public void  Long lastOffset = _lastOffset;

  /**
   * Find the largest offset less than or equal to the given targetOffset 
   * and return a pair holding this offset and its corresponding physical file position.
   * 
   * @param targetOffset The offset to look up.
   * @return The offset found and the corresponding file position for this offset
   *         If the target offset is smaller than the least entry in the index (or the index is empty),
   *         the pair (baseOffset, 0) is returned.
   */
  public void  lookup(Long targetOffset): OffsetPosition = {
    maybeLock(lock) {
      val idx = mmap.duplicate;
      val slot = largestLowerBoundSlotFor(idx, targetOffset, IndexSearchType.KEY);
      if(slot == -1)
        OffsetPosition(baseOffset, 0);
      else;
        parseEntry(idx, slot).asInstanceOf<OffsetPosition>;
    }
  }

  /**
   * Find an upper bound offset for the given fetch starting position and size. This is an offset which
   * is guaranteed to be outside the fetched range, but note that it will not generally be the smallest
   * such offset.
   */
  public void  fetchUpperBoundOffset(OffsetPosition fetchOffset, Integer fetchSize): Option<OffsetPosition> = {
    maybeLock(lock) {
      val idx = mmap.duplicate;
      val slot = smallestUpperBoundSlotFor(idx, fetchOffset.position + fetchSize, IndexSearchType.VALUE);
      if (slot == -1)
        None;
      else;
        Some(parseEntry(idx, slot).asInstanceOf<OffsetPosition>);
    }
  }

  private public void  relativeOffset(ByteBuffer buffer, Integer n): Integer = buffer.getInt(n * entrySize);

  private public void  physical(ByteBuffer buffer, Integer n): Integer = buffer.getInt(n * entrySize + 4);

  override public void  parseEntry(ByteBuffer buffer, Integer n): IndexEntry = {
      OffsetPosition(baseOffset + relativeOffset(buffer, n), physical(buffer, n));
  }
  ;
  /**
   * Get the nth offset mapping from the index
   * @param n The entry number in the index
   * @return The offset/position pair at that entry
   */
  public void  entry Integer n): OffsetPosition = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException(String.format("Attempt to fetch the %dth entry from an index of size %d.",n, _entries))
      val idx = mmap.duplicate;
      OffsetPosition(relativeOffset(idx, n), physical(idx, n));
    }
  }

  /**
   * Append an entry for the given offset/location pair to the index. This entry must have a larger offset than all subsequent entries.
   */
  public void  append(Long offset, Integer position) {
    inLock(lock) {
      require(!isFull, "Attempt to append to a full index (size = " + _entries + ").");
      if (_entries == 0 || offset > _lastOffset) {
        debug(String.format("Adding index entry %d => %d to %s.",offset, position, file.getName))
        mmap.putInt((offset - baseOffset).toInt);
        mmap.putInt(position);
        _entries += 1;
        _lastOffset = offset;
        require(_entries * entrySize == mmap.position, entries + " entries but file position in index is " + mmap.position + ".");
      } else {
        throw new InvalidOffsetException("Attempt to append an offset (%d) to position %d no larger than the last offset appended (%d) to %s.";
          .format(offset, entries, _lastOffset, file.getAbsolutePath))
      }
    }
  }

  override public void  truncate() = truncateToEntries(0);

  override public void  truncateTo(Long offset) {
    inLock(lock) {
      val idx = mmap.duplicate;
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.KEY);

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries = ;
        if(slot < 0)
          0;
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot;
        else;
          slot + 1;
      truncateToEntries(newEntries);
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  private public void  truncateToEntries Integer entries) {
    inLock(lock) {
      _entries = entries;
      mmap.position(_entries * entrySize);
      _lastOffset = lastEntry.offset;
    }
  }

  override public void  sanityCheck() {
    require(_entries == 0 || _lastOffset > baseOffset,
            s"Corrupt index found, index file (${file.getAbsolutePath}) has non-zero size but the last offset " +;
                s"is ${_lastOffset} which is no larger than the base offset $baseOffset.");
    val len = file.length();
    require(len % entrySize == 0,
            "Index file " + file.getAbsolutePath + " is corrupt, found " + len +;
            " bytes which is not positive or not a multiple of 8.");
  }

}
