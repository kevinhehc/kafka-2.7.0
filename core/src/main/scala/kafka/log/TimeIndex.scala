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

package kafka.log

import java.io.File
import java.nio.ByteBuffer

import kafka.utils.CoreUtils.inLock
import kafka.utils.Logging
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.record.RecordBatch

/**
 * An index that maps from the timestamp to the logical offsets of the messages in a segment. This index might be
 * sparse, i.e. it may not hold an entry for all the messages in the segment.
 *
 * The index is stored in a file that is preallocated to hold a fixed maximum amount of 12-byte time index entries.
 * The file format is a series of time index entries. The physical format is a 8 bytes timestamp and a 4 bytes "relative"
 * offset used in the [[OffsetIndex]]. A time index entry (TIMESTAMP, OFFSET) means that the biggest timestamp seen
 * before OFFSET is TIMESTAMP. i.e. Any message whose timestamp is greater than TIMESTAMP must come after OFFSET.
 *
 * All external APIs translate from relative offsets to full offsets, so users of this class do not interact with the internal
 * storage format.
 *
 * The timestamps in the same time index file are guaranteed to be monotonically increasing.
 *
 * The index support timestamp lookup for a memory map of this file. The lookup is done using a binary search to find
 * the offset of the message whose indexed timestamp is closest but smaller or equals to the target timestamp.
 *
 * Time index files can be opened in two ways: either as an empty, mutable index that allows appends or
 * an immutable read-only index file that has previously been populated. The makeReadOnly method will turn a mutable file into an
 * immutable one and truncate off any extra bytes. This is done when the index file is rolled over.
 *
 * No attempt is made to checksum the contents of this file, in the event of a crash it is rebuilt.
 *
 */
// Avoid shadowing mutable file in AbstractIndex
// file 索引文件：每个索引对象在磁盘上都对应了一个索引文件。
// baseOffset 起始位移值：对应日志文件中第一个消息的offset。
// maxIndexSize 索引文件最大字节数：它控制索引文件的最大长度，这里为 -1。
// writable 索引文件打开方式：这里为 true 表示以读写方式打开。
// mmap：用来操作索引文件的 MappedByteBuffer。
// lock：ReentrantLock 对象，在对 mmap 进行操作时，需要加锁保护。
// lastEntries：当前索引文件中最后一个索引项。
class TimeIndex(_file: File, baseOffset: Long, maxIndexSize: Int = -1, writable: Boolean = true)
    extends AbstractIndex(_file, baseOffset, maxIndexSize, writable) {
  import TimeIndex._

  @volatile private var _lastEntry = lastEntryFromIndexFile

  override def entrySize = 12

  debug(s"Loaded index file ${file.getAbsolutePath} with maxEntries = $maxEntries, maxIndexSize = $maxIndexSize," +
    s" entries = ${_entries}, lastOffset = ${_lastEntry}, file position = ${mmap.position()}")

  // We override the full check to reserve the last time index entry slot for the on roll call.
  override def isFull: Boolean = entries >= maxEntries - 1

  private def timestamp(buffer: ByteBuffer, n: Int): Long = buffer.getLong(n * entrySize)

  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * entrySize + 8)

  def lastEntry: TimestampOffset = _lastEntry

  /**
   * Read the last entry from the index file. This operation involves disk access.
   */
  private def lastEntryFromIndexFile: TimestampOffset = {
    inLock(lock) {
      _entries match {
        case 0 => TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
        case s => parseEntry(mmap, s - 1)
      }
    }
  }

  /**
   * Get the nth timestamp mapping from the time index
   * @param n The entry number in the time index
   * @return The timestamp/offset pair at that entry
   */
  def entry(n: Int): TimestampOffset = {
    maybeLock(lock) {
      if(n >= _entries)
        throw new IllegalArgumentException(s"Attempt to fetch the ${n}th entry from  time index ${file.getAbsolutePath} " +
          s"which has size ${_entries}.")
      parseEntry(mmap, n)
    }
  }

  override def parseEntry(buffer: ByteBuffer, n: Int): TimestampOffset = {
    TimestampOffset(timestamp(buffer, n), baseOffset + relativeOffset(buffer, n))
  }

  /**
   * Attempt to append a time index entry to the time index.
   * The new entry is appended only if both the timestamp and offsets are greater than the last appended timestamp and
   * the last appended offset.
   *
   * @param timestamp The timestamp of the new time index entry
   * @param offset The offset of the new time index entry
   * @param skipFullCheck To skip checking whether the segment is full or not. We only skip the check when the segment
   *                      gets rolled or the segment is closed.
   */
  def maybeAppend(timestamp: Long, offset: Long, skipFullCheck: Boolean = false): Unit = {
    inLock(lock) {
      // 如果索引文件已写满，抛出异常
      if (!skipFullCheck)
        require(!isFull, "Attempt to append to a full time index (size = " + _entries + ").")
      // We do not throw exception when the offset equals to the offset of last entry. That means we are trying
      // to insert the same time index entry as the last entry.
      // If the timestamp index entry to be inserted is the same as the last entry, we simply ignore the insertion
      // because that could happen in the following two scenarios:
      // 1. A log segment is closed.
      // 2. LogSegment.onBecomeInactiveSegment() is called when an active log segment is rolled.
      // 确保索引单调增加性
      if (_entries != 0 && offset < lastEntry.offset)
        throw new InvalidOffsetException(s"Attempt to append an offset ($offset) to slot ${_entries} no larger than" +
          s" the last offset appended (${lastEntry.offset}) to ${file.getAbsolutePath}.")
      // 确保时间戳的单调增加性
      if (_entries != 0 && timestamp < lastEntry.timestamp)
        throw new IllegalStateException(s"Attempt to append a timestamp ($timestamp) to slot ${_entries} no larger" +
          s" than the last timestamp appended (${lastEntry.timestamp}) to ${file.getAbsolutePath}.")
      // We only append to the time index when the timestamp is greater than the last inserted timestamp.
      // If all the messages are in message format v0, the timestamp will always be NoTimestamp. In that case, the time
      // index will be empty.
      // 符合单调递增性
      if (timestamp > lastEntry.timestamp) {
        trace(s"Adding index entry $timestamp => $offset to ${file.getAbsolutePath}.")
        // 向 mmap 写入时间戳
        mmap.putLong(timestamp)
        // 向 mmap 写入相对位移值
        mmap.putInt(relativeOffset(offset))
        // 更新其他元数据统计信息，当前索引项计数器 _entries 和 当前条目设置为最新的条目_lastEntry。
        _entries += 1
        _lastEntry = TimestampOffset(timestamp, offset)
        require(_entries * entrySize == mmap.position(), s"${_entries} entries but file position in index is ${mmap.position()}.")
      }
    }
  }

  /**
   * Find the time index entry whose timestamp is less than or equal to the given timestamp.
   * If the target timestamp is smaller than the least timestamp in the time index, (NoTimestamp, baseOffset) is
   * returned.
   *
   * @param targetTimestamp The timestamp to look up.
   * @return The time index entry found.
   */
  def lookup(targetTimestamp: Long): TimestampOffset = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, targetTimestamp, IndexSearchType.KEY)
      if (slot == -1)
        TimestampOffset(RecordBatch.NO_TIMESTAMP, baseOffset)
      else
        parseEntry(idx, slot)
    }
  }

  // 覆写 AbstractIndex 类的 truncate 方法，将索引文件截断至第一条索引记录之前，即清空索引文件。
  override def truncate() = truncateToEntries(0)

  /**
   * Remove all entries from the index which have an offset greater than or equal to the given offset.
   * Truncating to an offset larger than the largest in the index has no effect.
   */
  override def truncateTo(offset: Long): Unit = {
    inLock(lock) {
      val idx = mmap.duplicate
      val slot = largestLowerBoundSlotFor(idx, offset, IndexSearchType.VALUE)

      /* There are 3 cases for choosing the new size
       * 1) if there is no entry in the index <= the offset, delete everything
       * 2) if there is an entry for this exact offset, delete it and everything larger than it
       * 3) if there is no entry for this offset, delete everything larger than the next smallest
       */
      val newEntries =
        if(slot < 0)
          0
        else if(relativeOffset(idx, slot) == offset - baseOffset)
          slot
        else
          slot + 1
      truncateToEntries(newEntries)
    }
  }

  override def resize(newSize: Int): Boolean = {
    inLock(lock) {
      if (super.resize(newSize)) {
        _lastEntry = lastEntryFromIndexFile
        true
      } else
        false
    }
  }

  /**
   * Truncates index to a known number of entries.
   */
  // 根据索引条目数截断索引文件
  private def truncateToEntries(entries: Int): Unit = {
    // 使用锁确保线程安全。
    inLock(lock) {
      // 将索引文件内存映射到指定大小。
      _entries = entries
      // 将文件指针设置到索引区的末尾。
      mmap.position(_entries * entrySize)
      // 更新索引文件中的 _lastOffset 字段，将其设置为当前索引中的最后一个条目的偏移量。
      _lastEntry = lastEntryFromIndexFile
      // 记录日志。该方法的作用是根据指定的索引条目数截断索引文件，并更新相应的元数据信息。
      debug(s"Truncated index ${file.getAbsolutePath} to $entries entries; position is now ${mmap.position()} and last entry is now ${_lastEntry}")
    }
  }

  override def sanityCheck(): Unit = {
    val lastTimestamp = lastEntry.timestamp
    val lastOffset = lastEntry.offset
    if (_entries != 0 && lastTimestamp < timestamp(mmap, 0))
      throw new CorruptIndexException(s"Corrupt time index found, time index file (${file.getAbsolutePath}) has " +
        s"non-zero size but the last timestamp is $lastTimestamp which is less than the first timestamp " +
        s"${timestamp(mmap, 0)}")
    if (_entries != 0 && lastOffset < baseOffset)
      throw new CorruptIndexException(s"Corrupt time index found, time index file (${file.getAbsolutePath}) has " +
        s"non-zero size but the last offset is $lastOffset which is less than the first offset $baseOffset")
    if (length % entrySize != 0)
      throw new CorruptIndexException(s"Time index file ${file.getAbsolutePath} is corrupt, found $length bytes " +
        s"which is neither positive nor a multiple of $entrySize.")
  }
}

object TimeIndex extends Logging {
  override val loggerName: String = classOf[TimeIndex].getName
}
