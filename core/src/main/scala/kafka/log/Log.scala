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

import java.io.{File, IOException}
import java.lang.{Long => JLong}
import java.nio.file.{Files, NoSuchFileException}
import java.text.NumberFormat
import java.util.Map.{Entry => JEntry}
import java.util.Optional
import java.util.concurrent.atomic._
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap, TimeUnit}
import java.util.regex.Pattern

import kafka.api.{ApiVersion, KAFKA_0_10_0_IV0}
import kafka.common.{LogSegmentOffsetOverflowException, LongRef, OffsetsOutOfOrderException, UnexpectedAppendOffsetException}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.LeaderEpochCheckpointFile
import kafka.server.epoch.LeaderEpochFileCache
import kafka.server.{BrokerTopicStats, FetchDataInfo, FetchHighWatermark, FetchIsolation, FetchLogEnd, FetchTxnCommitted, LogDirFailureChannel, LogOffsetMetadata, OffsetAndEpoch}
import kafka.utils._
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.requests.{EpochEndOffset, ListOffsetRequest}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{InvalidRecordException, KafkaException, TopicPartition}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Seq, Set, mutable}

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
    RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false, -1L)

  def unknownLogAppendInfoWithLogStartOffset(logStartOffset: Long): LogAppendInfo =
    LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1,
      offsetsMonotonic = false, -1L)

  /**
   * In ProduceResponse V8+, we add two new fields record_errors and error_message (see KIP-467).
   * For any record failures with InvalidTimestamp or InvalidRecordException, we construct a LogAppendInfo object like the one
   * in unknownLogAppendInfoWithLogStartOffset, but with additiona fields recordErrors and errorMessage
   */
  def unknownLogAppendInfoWithAdditionalInfo(logStartOffset: Long, recordErrors: Seq[RecordError], errorMessage: String): LogAppendInfo =
    LogAppendInfo(None, -1, RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, NoCompressionCodec, NoCompressionCodec, -1, -1,
      offsetsMonotonic = false, -1L, recordErrors, errorMessage)
}

sealed trait LeaderHwChange
object LeaderHwChange {
  case object Increased extends LeaderHwChange
  case object Same extends LeaderHwChange
  case object None extends LeaderHwChange
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset The first offset in the message set unless the message format is less than V2 and we are appending
 *                    to the follower.
 * @param lastOffset The last offset in the message set
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param logStartOffset The start offset of the log at the time of this append.
 * @param recordConversionStats Statistics collected during record processing, `null` if `assignOffsets` is `false`
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 * @param lastOffsetOfFirstBatch The last offset of the first batch
 * @param leaderHwChange Incremental if the high watermark needs to be increased after appending record.
 *                       Same if high watermark is not changed. None is the default value and it means append failed
 *
 */
case class LogAppendInfo(var firstOffset: Option[Long], // 消息集合第一条消息的位移值
                         var lastOffset: Long, // 消息集合最后一条消息的位移值
                         var maxTimestamp: Long, // 消息集合最大消息时间戳
                         var offsetOfMaxTimestamp: Long, // 消息集合最大消息时间戳所属消息的位移值
                         var logAppendTime: Long, // 写入消息时间戳
                         var logStartOffset: Long, // 消息集合首条消息的位移值
                         var recordConversionStats: RecordConversionStats, // 消息转换统计类，里面记录了执行了格式转换的消息数等数据
                         sourceCodec: CompressionCodec, // 消息集合中消息使用的压缩器（Compressor）类型，比如是Snappy还是LZ4
                         targetCodec: CompressionCodec, // 写入消息时需要使用的压缩器类型
                         shallowCount: Int, // 消息批次数，每个消息批次下可能包含多条消息
                         validBytes: Int, // 写入消息总字节数
                         offsetsMonotonic: Boolean, // 消息位移值是否是顺序增加的
                         lastOffsetOfFirstBatch: Long, // 首个消息批次中最后一条消息的位移
                         recordErrors: Seq[RecordError] = List(), // 写入消息时出现的异常列表
                         errorMessage: String = null, // 错误码
                         leaderHwChange: LeaderHwChange = LeaderHwChange.None) {
  /**
   * Get the first offset if it exists, else get the last offset of the first batch
   * For magic versions 2 and newer, this method will return first offset. For magic versions
   * older than 2, we use the last offset of the first batch as an approximation of the first
   * offset to avoid decompressing the data.
   */
  def firstOrLastOffsetOfFirstBatch: Long = firstOffset.getOrElse(lastOffsetOfFirstBatch)

  /**
   * Get the (maximum) number of messages described by LogAppendInfo
   * @return Maximum possible number of messages described by LogAppendInfo
   */
  def numMessages: Long = {
    firstOffset match {
      case Some(firstOffsetVal) if (firstOffsetVal >= 0 && lastOffset >= 0) => (lastOffset - firstOffsetVal + 1)
      case _ => 0
    }
  }
}

/**
 * Container class which represents a snapshot of the significant offsets for a partition. This allows fetching
 * of these offsets atomically without the possibility of a leader change affecting their consistency relative
 * to each other. See [[Log.fetchOffsetSnapshot()]].
 */
case class LogOffsetSnapshot(logStartOffset: Long,
                             logEndOffset: LogOffsetMetadata,
                             highWatermark: LogOffsetMetadata,
                             lastStableOffset: LogOffsetMetadata)

/**
 * Another container which is used for lower level reads using  [[kafka.cluster.Partition.readRecords()]].
 */
case class LogReadInfo(fetchedData: FetchDataInfo,
                       divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                       highWatermark: Long,
                       logStartOffset: Long,
                       logEndOffset: Long,
                       lastStableOffset: Long)

/**
 * A class used to hold useful metadata about a completed transaction. This is used to build
 * the transaction index after appending to the log.
 *
 * @param producerId The ID of the producer
 * @param firstOffset The first offset (inclusive) of the transaction
 * @param lastOffset The last offset (inclusive) of the transaction. This is always the offset of the
 *                   COMMIT/ABORT control record which indicates the transaction's completion.
 * @param isAborted Whether or not the transaction was aborted
 */
case class CompletedTxn(producerId: Long, firstOffset: Long, lastOffset: Long, isAborted: Boolean) {
  override def toString: String = {
    "CompletedTxn(" +
      s"producerId=$producerId, " +
      s"firstOffset=$firstOffset, " +
      s"lastOffset=$lastOffset, " +
      s"isAborted=$isAborted)"
  }
}

/**
 * A class used to hold params required to decide to rotate a log segment or not.
 */
case class RollParams(maxSegmentMs: Long,
                      maxSegmentBytes: Int,
                      maxTimestampInMessages: Long,
                      maxOffsetInMessages: Long,
                      messagesSize: Int,
                      now: Long)

object RollParams {
  def apply(config: LogConfig, appendInfo: LogAppendInfo, messagesSize: Int, now: Long): RollParams = {
   new RollParams(config.maxSegmentMs,
     config.segmentSize,
     appendInfo.maxTimestamp,
     appendInfo.lastOffset,
     messagesSize, now)
  }
}

sealed trait LogStartOffsetIncrementReason
case object ClientRecordDeletion extends LogStartOffsetIncrementReason {
  override def toString: String = "client delete records request"
}
case object LeaderOffsetIncremented extends LogStartOffsetIncrementReason {
  override def toString: String = "leader offset increment"
}
case object SegmentDeletion extends LogStartOffsetIncrementReason {
  override def toString: String = "segment deletion"
}

/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param _dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param logStartOffset The earliest offset allowed to be exposed to kafka client.
 *                       The logStartOffset can be updated by :
 *                       - user's DeleteRecordsRequest
 *                       - broker's log retention
 *                       - broker's log truncation
 *                       The logStartOffset is used to decide the following:
 *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
 *                         It may trigger log rolling if the active segment is deleted.
 *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
 *                         we make sure that logStartOffset <= log's highWatermark
 *                       Other activities such as log cleaning are not affected by logStartOffset.
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param brokerTopicStats Container for Broker Topic Yammer Metrics
 * @param time The time instance used for checking the clock
 * @param maxProducerIdExpirationMs The maximum amount of time to wait before a producer id is considered expired
 * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
 */
// 高水位管理：对高水位概念的定义以及对高水位的各种操作，比如：设置高水位、读取高水位、更新高水位等。
// 日志段管理：对日志段的增删改查操作，了解日志是如何对日志段进行管理的。
// 关键位移值管理：主要针对 LEO、LSO 位移值的管理。
// 日志读写操作：日志读写操作，这是 Kafka 消息中间件的基石。
@threadsafe
class Log(@volatile private var _dir: File, // 当前日志目录
          @volatile var config: LogConfig, // 当前日志的配置信息
          @volatile var logStartOffset: Long, // 当前日志起始偏移量
          @volatile var recoveryPoint: Long, // 需要恢复的数据的位置偏移量
          scheduler: Scheduler, // 用于调度一些异步任务的 scheduler。
          brokerTopicStats: BrokerTopicStats, // 记录 kafka Broker 级别的一些统计信息。
          val time: Time, // kafka 时间类库，用于获取当前时间等操作。
          val maxProducerIdExpirationMs: Int, //  设置 Producer ID 的最大过期时间。
          val producerIdExpirationCheckIntervalMs: Int, // 设置检查 Producer ID 过期的时间间隔。
          val topicPartition: TopicPartition, // 当前日志所属的主题分区
          val producerStateManager: ProducerStateManager,  // 生产者状态管理器

          // Kafka 监测 Log 目录故障的通道。
          logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {

  // 导入一些 Log 静态的常量和方法
  import kafka.log.Log._

  this.logIdent = s"[Log partition=$topicPartition, dir=${dir.getParent}] "

  /* A lock that guards all modifications to the log */
  // 创建一个锁对象
  private val lock = new Object

  // The memory mapped buffer for index files of this log will be closed with either delete() or closeHandlers()
  // After memory mapped buffer is closed, no disk IO operation should be performed for this log
  // 是否已经关闭了内存映射的缓冲区，默认否
  @volatile private var isMemoryMappedBufferClosed = false

  // Cache value of parent directory to avoid allocations in hot paths like ReplicaManager.checkpointHighWatermarks
  // 记录父目录路径的字符串形式
  @volatile private var _parentDir: String = dir.getParent

  /* last time it was flushed */
  // 上一次flush的时间，用于日志管理中定期将内存数据刷入磁盘，这里使用 AtomicLong 以实现线程安全
  private val lastFlushedTime = new AtomicLong(time.milliseconds)

  // 下一个消息起始位置的元数据信息，使用 volatile 修饰，同时使用 LogOffsetMetadata 包含了当前的三个重要属性值
  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* The earliest offset which is part of an incomplete transaction. This is used to compute the
   * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
   * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
   * will point to the log start offset, which may actually be either part of a completed transaction or not
   * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
   * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
   * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
   * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
   * that this could result in disagreement between replicas depending on when they began replicating the log.
   * In the worst case, the LSO could be seen by a consumer to go backwards.
   */
  // 第一个不稳定的位置元数据信息，使用 Option 包裹，为空值时表示没有不稳定的位置
  @volatile private var firstUnstableOffsetMetadata: Option[LogOffsetMetadata] = None

  /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
   * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
   * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
   * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
   */
  // 高水位元数据信息
  @volatile private var highWatermarkMetadata: LogOffsetMetadata = LogOffsetMetadata(logStartOffset)

  /* the actual segments of the log */
  // Kafka 的 Log 由顺序的 Segment 日志段组成，这里使用 ConcurrentSkipListMap 存储，并且会基于消息的偏移量进行排序
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]

  // Visible for testing
  // 为 TopicPartition 构建 LeaderEpoch => Offset 映射缓存
  @volatile var leaderEpochCache: Option[LeaderEpochFileCache] = None

  // 文件夹的初始化操作
  locally {
    // create the log directory if it doesn't exist
    // 如果目录不存在的话则创建目录
    Files.createDirectories(dir.toPath)

    // 初始化 LeaderEpochCache
    initializeLeaderEpochCache()

    // 加载所有的日志段，得到下一个消息的起始位置
    val nextOffset = loadSegments()

    /* Calculate the offset of the next message */
    // 初始化 nextOffsetMetadata 元数据信息
    nextOffsetMetadata = LogOffsetMetadata(nextOffset, activeSegment.baseOffset, activeSegment.size)

    // 重置 leaderEpochCache，从末尾开始截取
    // 更新 LeaderEpoch 缓存，清除大于等于 LEO 值的所有无效缓存项
    leaderEpochCache.foreach(_.truncateFromEnd(nextOffsetMetadata.messageOffset))

    // 更新日志起始位置
    updateLogStartOffset(math.max(logStartOffset, segments.firstEntry.getValue.baseOffset))

    // The earliest leader epoch may not be flushed during a hard failure. Recover it here.
    // 重置 leaderEpochCache，从头开始截取
    // 更新 LeaderEpoch 缓存，清除小于等于 LSO 值的所有无效缓存项
    leaderEpochCache.foreach(_.truncateFromStart(logStartOffset))

    // Any segment loading or recovery code must not use producerStateManager, so that we can build the full state here
    // from scratch.
    // 加载生产者状态，如果存在异常的状态则会抛出异常信息
    if (!producerStateManager.isEmpty)
      throw new IllegalStateException("Producer state must be empty during log initialization")
    loadProducerState(logEndOffset, reloadFromCleanShutdown = hasCleanShutdownFile)
  }

  def dir: File = _dir

  def parentDir: String = _parentDir

  def parentDirFile: File = new File(_parentDir)

  def initFileSize: Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  def updateConfig(newConfig: LogConfig): Unit = {
    val oldConfig = this.config
    this.config = newConfig
    val oldRecordVersion = oldConfig.messageFormatVersion.recordVersion
    val newRecordVersion = newConfig.messageFormatVersion.recordVersion
    if (newRecordVersion.precedes(oldRecordVersion))
      warn(s"Record format version has been downgraded from $oldRecordVersion to $newRecordVersion.")
    if (newRecordVersion.value != oldRecordVersion.value)
      initializeLeaderEpochCache()
  }

  private def checkIfMemoryMappedBufferClosed(): Unit = {
    if (isMemoryMappedBufferClosed)
      throw new KafkaStorageException(s"The memory mapped buffer for log of $topicPartition is already closed")
  }

  // 读取高水位的位移值
  def highWatermark: Long = highWatermarkMetadata.messageOffset

  /**
   * Update the high watermark to a new offset. The new high watermark will be lower
   * bounded by the log start offset and upper bounded by the log end offset.
   *
   * This is intended to be called when initializing the high watermark or when updating
   * it on a follower after receiving a Fetch response from the leader.
   *
   * @param hw the suggested new value for the high watermark
   * @return the updated high watermark offset
   */
  // 更新高水位值，新高水位值一定介于[Log Start Offset，Log End Offset]之间
  def updateHighWatermark(hw: Long): Long = {
    // 如果新的高水位偏移量小于当前日志段的起始偏移量，则将新的高水位偏移量设为当前日志段的起始偏移量。
    val newHighWatermark = if (hw < logStartOffset)
      logStartOffset
    // 如果新的高水位偏移量大于当前日志段的末端偏移量，则将新的高水位偏移量设为当前日志段的末端偏移量。
    else if (hw > logEndOffset)
      logEndOffset
    // 否则，将新的高水位偏移量设为输入值 hw。
    else
      hw
    // 以新的高水位偏移量为参数更新高水位元数据
    updateHighWatermarkMetadata(LogOffsetMetadata(newHighWatermark))
    // 最后返回新高水位值
    newHighWatermark
  }

  def updateHighWatermarkOffsetMetadata(hw: LogOffsetMetadata): Long = {
    val newHighWatermark = if (hw.messageOffset < logStartOffset) {
      updateHighWatermarkMetadata(LogOffsetMetadata(logStartOffset))
      logStartOffset
    } else if (hw.messageOffset > logEndOffset) {
      updateHighWatermarkMetadata(logEndOffsetMetadata)
      logEndOffset
    } else {
      updateHighWatermarkMetadata(hw)
      hw.messageOffset
    }
    newHighWatermark
  }

  /**
   * Update the high watermark to a new value if and only if it is larger than the old value. It is
   * an error to update to a value which is larger than the log end offset.
   *
   * This method is intended to be used by the leader to update the high watermark after follower
   * fetch offsets have been updated.
   *
   * @return the old high watermark, if updated by the new value
   */
  // 尝试增加高水位偏移量元数据，如果成功增加了高水位偏移量元数据，则返回先前的高水位元数据
  def maybeIncrementHighWatermark(newHighWatermark: LogOffsetMetadata): Option[LogOffsetMetadata] = {
    // 新高水位值不能越过 LogEndOffset，否则抛异常
    if (newHighWatermark.messageOffset > logEndOffset)
      throw new IllegalArgumentException(s"High watermark $newHighWatermark update exceeds current " +
        s"log end offset $logEndOffsetMetadata")

    // 保护 Log 对象修改的 Monitor 锁
    lock.synchronized {
      // 获取旧的高水位值
      val oldHighWatermark = fetchHighWatermarkMetadata

      // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
      // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
      // 首先，如果新的高水位偏移量大于旧的高水位偏移量，则更新高水位元数据为新的高水位元数据，并返回旧的高水位元数据
      // 其次，如果旧的高水位元数据与新的高水位元数据相等并且旧的高水位元数据比新的高水位元数据旧，
      // 则更新高水位元数据为新的高水位元数据，并返回旧的高水位元数据
      if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||
        (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
        // 更新高水位元数据
        updateHighWatermarkMetadata(newHighWatermark)
        // 返回老的高水位值
        Some(oldHighWatermark)
      } else {
        // 如果新的高水位元数据不大于旧的高水位元数据，则返回 None
        None
      }
    }
  }

  /**
   * Get the offset and metadata for the current high watermark. If offset metadata is not
   * known, this will do a lookup in the index and cache the result.
   */
  private def fetchHighWatermarkMetadata: LogOffsetMetadata = {
    // 读取时确保日志不能被关闭
    checkIfMemoryMappedBufferClosed()

    // 保存当前高水位值到本地变量，避免多线程访问干扰
    val offsetMetadata = highWatermarkMetadata
    // 当没有获得到完整的高水位元数据
    if (offsetMetadata.messageOffsetOnly) {
      lock.synchronized {
        // 通过读日志文件的方式把完整的高水位元数据信息拉出来
        val fullOffset = convertToOffsetMetadataOrThrow(highWatermark)
        // 然后再更新一下高水位对象
        updateHighWatermarkMetadata(fullOffset)
        // 返回完整的高水位元数据信息
        fullOffset
      }
    } else {
      // 否则直接返回
      offsetMetadata
    }
  }

  // 设置高水位元数据
  private def updateHighWatermarkMetadata(newHighWatermark: LogOffsetMetadata): Unit = {
    // 判断高水位值是不是负数，否则抛异常
    if (newHighWatermark.messageOffset < 0)
      throw new IllegalArgumentException("High watermark offset should be non-negative")

    // 保护 Log 对象修改的 Monitor 锁
    lock synchronized {
      // 赋值新的高水位值
      highWatermarkMetadata = newHighWatermark
      // 通知生产者的状态管理器更新高水位偏移量
      producerStateManager.onHighWatermarkUpdated(newHighWatermark.messageOffset)
      // 尝试增加第一个不稳定偏移量的值。以便在后续的操作中保证事务在提交前同步到磁盘上的数据不会被回滚。
      // 该方法首先检查是否关闭了内存映射缓冲，然后获取 ProducerStateManager 实例中缓存的第一个不稳定偏移量元数据，
      // 并根据当前日志段的起始偏移量对该元数据进行更新。最后更新 firstUnstableOffsetMetadata 元数据缓存。
      maybeIncrementFirstUnstableOffset()
    }
    trace(s"Setting high watermark $newHighWatermark")
  }

  /**
   * Get the first unstable offset. Unlike the last stable offset, which is always defined,
   * the first unstable offset only exists if there are transactions in progress.
   *
   * @return the first unstable offset, if it exists
   */
  private[log] def firstUnstableOffset: Option[Long] = firstUnstableOffsetMetadata.map(_.messageOffset)

  private def fetchLastStableOffsetMetadata: LogOffsetMetadata = {
    checkIfMemoryMappedBufferClosed()

    // cache the current high watermark to avoid a concurrent update invalidating the range check
    val highWatermarkMetadata = fetchHighWatermarkMetadata

    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermarkMetadata.messageOffset =>
        if (offsetMetadata.messageOffsetOnly) {
          lock synchronized {
            val fullOffset = convertToOffsetMetadataOrThrow(offsetMetadata.messageOffset)
            if (firstUnstableOffsetMetadata.contains(offsetMetadata))
              firstUnstableOffsetMetadata = Some(fullOffset)
            fullOffset
          }
        } else {
          offsetMetadata
        }
      case _ => highWatermarkMetadata
    }
  }

  /**
   * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
   * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
   * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
   * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
   * beyond the high watermark.
   */
  def lastStableOffset: Long = {
    firstUnstableOffsetMetadata match {
      case Some(offsetMetadata) if offsetMetadata.messageOffset < highWatermark => offsetMetadata.messageOffset
      case _ => highWatermark
    }
  }

  def lastStableOffsetLag: Long = highWatermark - lastStableOffset

  /**
    * Fully materialize and return an offset snapshot including segment position info. This method will update
    * the LogOffsetMetadata for the high watermark and last stable offset if they are message-only. Throws an
    * offset out of range error if the segment info cannot be loaded.
    */
  def fetchOffsetSnapshot: LogOffsetSnapshot = {
    val lastStable = fetchLastStableOffsetMetadata
    val highWatermark = fetchHighWatermarkMetadata

    LogOffsetSnapshot(
      logStartOffset,
      logEndOffsetMetadata,
      highWatermark,
      lastStable
    )
  }

  private val tags = {
    val maybeFutureTag = if (isFuture) Map("is-future" -> "true") else Map.empty[String, String]
    Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString) ++ maybeFutureTag
  }

  newGauge(LogMetricNames.NumLogSegments, () => numberOfSegments, tags)
  newGauge(LogMetricNames.LogStartOffset, () => logStartOffset, tags)
  newGauge(LogMetricNames.LogEndOffset, () => logEndOffset, tags)
  newGauge(LogMetricNames.Size, () => size, tags)

  val producerExpireCheck = scheduler.schedule(name = "PeriodicProducerExpirationCheck", fun = () => {
    lock synchronized {
      producerStateManager.removeExpiredProducers(time.milliseconds)
    }
  }, period = producerIdExpirationCheckIntervalMs, delay = producerIdExpirationCheckIntervalMs, unit = TimeUnit.MILLISECONDS)

  /** The name of this log */
  def name  = dir.getName()

  def recordVersion: RecordVersion = config.messageFormatVersion.recordVersion

  // 从 leader-epoch-checkpoint 文件中加载 LeaderEpoch 信息并存储到 LeaderEpochFileCache#epochs 中。
  private def initializeLeaderEpochCache(): Unit = lock synchronized {
    // 创建用于存储 Leader Epoch 的检查点文件
    val leaderEpochFile = LeaderEpochCheckpointFile.newFile(dir)

    // 创建新的 Leader Epoch 文件缓存
    def newLeaderEpochFileCache(): LeaderEpochFileCache = {
      val checkpointFile = new LeaderEpochCheckpointFile(leaderEpochFile, logDirFailureChannel)
      new LeaderEpochFileCache(topicPartition, () => logEndOffset, checkpointFile)
    }

    // 如果记录版本在 V2 之前，则删除旧的 Leader Epoch 缓存，并删除检查点文件
    if (recordVersion.precedes(RecordVersion.V2)) {
      val currentCache = if (leaderEpochFile.exists())
        Some(newLeaderEpochFileCache())
      else
        None

      if (currentCache.exists(_.nonEmpty))
        warn(s"Deleting non-empty leader epoch cache due to incompatible message format $recordVersion")

      // 如果当前缓存非空，则警告删除非空的 Leader Epoch 缓存，因为与不兼容的消息格式(recordVersion)有关
      Files.deleteIfExists(leaderEpochFile.toPath)
      leaderEpochCache = None
    } else {
      // 创建新的 Leader Epoch 缓存
      leaderEpochCache = Some(newLeaderEpochFileCache())
    }
  }

  /**
   * Removes any temporary files found in log directory, and creates a list of all .swap files which could be swapped
   * in place of existing segment(s). For log splitting, we know that any .swap file whose base offset is higher than
   * the smallest offset .clean file could be part of an incomplete split operation. Such .swap files are also deleted
   * by this method.
   * @return Set of .swap files that are valid to be swapped in as segment files
   */
  private def removeTempFilesAndCollectSwapFiles(): Set[File] = {

    // 用来删除日志文件对应的 offset、time 或者 transaction 索引文件
    def deleteIndicesIfExist(baseFile: File, suffix: String = ""): Unit = {
      info(s"Deleting index files with suffix $suffix for baseFile $baseFile")
      val offset = offsetFromFile(baseFile)
      Files.deleteIfExists(Log.offsetIndexFile(dir, offset, suffix).toPath)
      Files.deleteIfExists(Log.timeIndexFile(dir, offset, suffix).toPath)
      Files.deleteIfExists(Log.transactionIndexFile(dir, offset, suffix).toPath)
    }

    // 定义一个空的可修改的 Set 对象 swapFiles，用于收集 swap 文件。
    val swapFiles = mutable.Set[File]()
    // 定义一个空的可修改的 Set 对象 cleanFiles，用于收集清理后的 Log 日志文件。
    val cleanFiles = mutable.Set[File]()
    // 定义一个 Long 类型的变量 minCleanedFileOffset，表示上一次清理的最小 offset 值，默认值为最大值。
    var minCleanedFileOffset = Long.MaxValue

    // 遍历日志路径下的所有文件
    for (file <- dir.listFiles if file.isFile) {
      // 如果不可读，则直接抛异常
      if (!file.canRead)
        throw new IOException(s"Could not read file $file")
      // 获取文件名
      val filename = file.getName
      // 如果文件名以 .deleted 结尾
      if (filename.endsWith(DeletedFileSuffix)) {
        debug(s"Deleting stray temporary file ${file.getAbsolutePath}")
        // 存在则说明是上次 Failure 遗留下来的临时文件，直接删除。
        Files.deleteIfExists(file.toPath)
      } else if (filename.endsWith(CleanedFileSuffix)) {
        // 如果文件名以 .cleaned 结尾

        // 从文件名 filename 上获取到 minCleanedFileOffset，
        // 此处如果修改了文件名的 offset 后会导致 Broker 崩溃无法启动，这里需要特别注意下。
        minCleanedFileOffset = Math.min(offsetFromFileName(filename), minCleanedFileOffset)
        cleanFiles += file
      } else if (filename.endsWith(SwapFileSuffix)) {
        // 如果文件名以 .swap 结尾

        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the index files, complete the swap operation later
        // if an index just delete the index files, they will be rebuilt
        val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        info(s"Found file ${file.getAbsolutePath} from interrupted swap operation.")
        // 如果该 swap 文件对应的是索引文件
        if (isIndexFile(baseFile)) {
          // 删除原来的索引文件
          deleteIndicesIfExist(baseFile)
        } else if (isLogFile(baseFile)) {
          // 如果该 swap 文件对应的是 log 文件

          // 删除原来的索引文件
          deleteIndicesIfExist(baseFile)

          // 加入待恢复的.swap文件集合中
          swapFiles += file
        }
      }
    }

    // KAFKA-6264: Delete all .swap files whose base offset is greater than the minimum .cleaned segment offset. Such .swap
    // files could be part of an incomplete split operation that could not complete. See Log#splitOverflowedSegment
    // for more details about the split operation.

    // 从待恢复 swap 集合中找出那些起始位移值大于 minCleanedFileOffset 值的文件，然后遍历删掉这些无效的 .swap 文件
    val (invalidSwapFiles, validSwapFiles) = swapFiles.partition(file => offsetFromFile(file) >= minCleanedFileOffset)
    invalidSwapFiles.foreach { file =>
      debug(s"Deleting invalid swap file ${file.getAbsoluteFile} minCleanedFileOffset: $minCleanedFileOffset")
      val baseFile = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
      deleteIndicesIfExist(baseFile, SwapFileSuffix)
      Files.deleteIfExists(file.toPath)
    }

    // Now that we have deleted all .swap files that constitute an incomplete split operation, let's delete all .clean files
    // 清除所有待删除文件集合中的文件
    cleanFiles.foreach { file =>
      debug(s"Deleting stray .clean file ${file.getAbsolutePath}")
      Files.deleteIfExists(file.toPath)
    }

    // 最后返回当前有效的.swap文件集合
    validSwapFiles
  }

  /**
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs are loaded
   * It is possible that we encounter a segment with index offset overflow in which case the LogSegmentOffsetOverflowException
   * will be thrown. Note that any segments that were opened before we encountered the exception will remain open and the
   * caller is responsible for closing them appropriately, if needed.
   * @throws LogSegmentOffsetOverflowException if the log directory contains a segment with messages that overflow the index offset
   */
  private def loadSegmentFiles(): Unit = {
    // load segments in ascending order because transactional data from one segment may depend on the
    // segments that come before it
    // 遍历 Log 目录下的所有文件，按照文件名排序后处理，只处理文件类型为 file 的文件。
    for (file <- dir.listFiles.sortBy(_.getName) if file.isFile) {
      // 如果是索引文件
      if (isIndexFile(file)) {
        // if it is an index file, make sure it has a corresponding .log file
        // 获取 Log 文件名对应的 offset 值。
        val offset = offsetFromFile(file)
        // 获取对应的 Log 文件。
        val logFile = Log.logFile(dir, offset)
        // 如果对应的 Log 文件不存在，则警告并删除该索引文件。
        if (!logFile.exists) {
          warn(s"Found an orphaned index file ${file.getAbsolutePath}, with no corresponding log file.")
          Files.deleteIfExists(file.toPath)
        }
      } else if (isLogFile(file)) {
        // 如果文件是 Log 文件

        // if it's a log file, load the corresponding log segment

        // 获取 Log 文件名对应的 offset 值。
        val baseOffset = offsetFromFile(file)
        // 判断该 Log 文件对应的 timeIndex 文件是否已存在，如果不存在则认为新创建文件。
        val timeIndexFileNewlyCreated = !Log.timeIndexFile(dir, baseOffset).exists()
        // 创建对应的 LogSegment 实例。
        val segment = LogSegment.open(dir = dir,
          baseOffset = baseOffset,
          config,
          time = time,
          fileAlreadyExists = true)

        // 对 LogSegment 进行检查，如果存在问题则抛出异常。
        try segment.sanityCheck(timeIndexFileNewlyCreated)
        catch {
          case _: NoSuchFileException =>
            error(s"Could not find offset index file corresponding to log file ${segment.log.file.getAbsolutePath}, " +
              "recovering segment and rebuilding index files...")
            recoverSegment(segment)
          case e: CorruptIndexException =>
            warn(s"Found a corrupted index file corresponding to log file ${segment.log.file.getAbsolutePath} due " +
              s"to ${e.getMessage}}, recovering segment and rebuilding index files...")
            recoverSegment(segment)
        }
        // 将该 LogSegment 添加到 Log 实例的 segments 列表中。
        addSegment(segment)
      }
    }
  }

  /**
   * Recover the given segment.
   * @param segment Segment to recover
   * @param leaderEpochCache Optional cache for updating the leader epoch during recovery
   * @return The number of bytes truncated from the segment
   * @throws LogSegmentOffsetOverflowException if the segment contains messages that cause index offset overflow
   */

  /*
   * 用于恢复给定的日志段
   * 该方法的作用是恢复给定的 LogSegment，并处理其中的数据。针对 ProducerStateManager 对象，
   * 恢复操作的具体实现是根据缓存或者文件中的状态重建生产者状态，并处理所有持久化到日志目录中的数据，
   * 以确保系统中的所有状态都正确地保存到了磁盘中。最后对 ProducerStateManager 进行快照以便于后续的恢复操作。
   * 同时，对于 leaderEpochCache 参数，该方法用它来更新消息的 leader 选举期，并将其存储到磁盘中。
   */
  private def recoverSegment(segment: LogSegment,
                             leaderEpochCache: Option[LeaderEpochFileCache] = None): Int = lock synchronized {
    // 创建 ProducerStateManager 对象，用于管理生产者状态。
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    // 使用 ProducerStateManager 对象重建生产者状态，根据 segment 的 baseOffset 从缓存或者文件恢复状态。
    rebuildProducerState(segment.baseOffset, reloadFromCleanShutdown = false, producerStateManager)
    // 调用 LogSegment 实例的 recover 方法来从 .log 文件和 .index 文件中读取数据，并将其加入到缓存中。
    // 对于 LeaderEpochFileCache 参数，这个实例可以用来更新日志段中消息的 leader 选举期存储。
    val bytesTruncated = segment.recover(producerStateManager, leaderEpochCache)
    // once we have recovered the segment's data, take a snapshot to ensure that we won't
    // need to reload the same segment again while recovering another segment.
    // 存储 ProducerStateManager 的快照，以便于后续恢复操作
    producerStateManager.takeSnapshot()
    // 返回从该日志段截断的字节数
    bytesTruncated
  }

  /**
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs
   * are loaded.
   * @throws LogSegmentOffsetOverflowException if the swap file contains messages that cause the log segment offset to
   *                                           overflow. Note that this is currently a fatal exception as we do not have
   *                                           a way to deal with it. The exception is propagated all the way up to
   *                                           KafkaServer#startup which will cause the broker to shut down if we are in
   *                                           this situation. This is expected to be an extremely rare scenario in practice,
   *                                           and manual intervention might be required to get out of it.
   */
  private def completeSwapOperations(swapFiles: Set[File]): Unit = {
    // 遍历所有有效的 .swap 文件
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      // 获取日志文件的起始位移值
      val baseOffset = offsetFromFile(logFile)
      // 创建对应的LogSegment实例
      val swapSegment = LogSegment.open(swapFile.getParentFile,
        baseOffset = baseOffset,
        config,
        time = time,
        fileSuffix = SwapFileSuffix)
      info(s"Found log file ${swapFile.getPath} from interrupted swap operation, repairing.")
      // 执行日志段恢复操作
      recoverSegment(swapSegment)

      // We create swap files for two cases:
      // (1) Log cleaning where multiple segments are merged into one, and
      // (2) Log splitting where one segment is split into multiple.
      //
      // Both of these mean that the resultant swap segments be composed of the original set, i.e. the swap segment
      // must fall within the range of existing segment(s). If we cannot find such a segment, it means the deletion
      // of that segment was successful. In such an event, we should simply rename the .swap to .log without having to
      // do a replace with an existing segment.
      // 确认之前删除日志段是否成功，是否还存在旧的日志段文件
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.readNextOffset).filter { segment =>
        segment.readNextOffset > swapSegment.baseOffset
      }
      // 如果存在的话直接把 .swap 文件重命名成 .log
      replaceSegments(Seq(swapSegment), oldSegments.toSeq, isRecoveredSwapFile = true)
    }
  }

  /**
   * Load the log segments from the log files on disk and return the next offset.
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all logs
   * are loaded.
   * @throws LogSegmentOffsetOverflowException if we encounter a .swap file with messages that overflow index offset; or when
   *                                           we find an unexpected number of .log files with overflow
   */

  /*
   * 从磁盘上的日志文件中加载日志段并返回下一个偏移量。此方法不需要将 IOException 转换为 KafkaStorageException，
   * 因为它只在加载所有日志之前调用。
   * 如果遇到具有溢出索引偏移的消息的 .swap 文件，则抛出 LogSegmentOffsetOverflowException 异常；
   * 或者当我们发现具有溢出的 .log 文件的数量时抛出异常
   */
  private def loadSegments(): Long = {
    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    // 1、首先对日志目录中的文件进行遍历，并清理上次 Failure 遗留下来的各种临时文件（包括以".delete"、".cleaned"、".swap" 结尾的文件），
    // 收集Swap文件并查找任何中断的 swap 操作。
    val swapFiles = removeTempFilesAndCollectSwapFiles()

    // Now do a second pass and load all the log and index files.
    // We might encounter legacy log segments with offset overflow (KAFKA-6264). We need to split such segments. When
    // this happens, restart loading segment files from scratch.
    // 现在进行第二次遍历，并加载所有的日志和索引文件
    // 我们可能会遇到具有偏移量溢出的旧日志段（KAFKA-6264）。
    // 我们需要拆分这样的段。当遇到这种情况时，重新从头开始加载段文件。
    retryOnOffsetOverflow {
      // In case we encounter a segment with offset overflow, the retry logic will split it after which we need to retry
      // loading of segments. In that case, we also need to close all segments that could have been left open in previous
      // call to loadSegmentFiles().
      // 如果遇到具有偏移量溢出的段，重试逻辑将对其进行拆分，然后我们需要重试加载段文件。
      // 在发起此次 loadSegmentFiles() 调用之前需要关闭所有被遗留的日志段
      logSegments.foreach(_.close())
      // 清空所有日志段对象
      segments.clear()
      // 再次遍历分区日志路径，载入 Segment 和 Index 文件，将 Segment依次加入 cache 中
      loadSegmentFiles()
    }

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    // 3、待执行完上面两次遍历后，完成恢复过程中发现任何中断的 swap 操作。
    // 载入 SwapSegment 并替换对应的 Segment，为了保证安全，
    // 被 swap 段取代的日志文件应该在恢复 swap 文件作为新段文件之前将其重命名为 .deleted，后面的定时任务或者下次的系统重启会删除。
    completeSwapOperations(swapFiles)

    // 如果当前目录是标准目录而不是被标记为“.deleted”，则恢复日志段对象、重置当前活跃日志段的索引大小、返回恢复之后的分区日志 LEO 值。
    if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
      val nextOffset = retryOnOffsetOverflow {
        // 根据 snapshot 恢复 Segment 各种缓存，根据 record 进行事务处理初始化等工作
        recoverLog()
      }

      // reset the index size of the currently active log segment to allow more entries
      // 重置当前活跃日志段的索引大小，以允许更多的条目。
      activeSegment.resizeIndexes(config.maxIndexSize)
      // 返回恢复之后的分区日志 LEO 值。
      nextOffset
    } else {
      // 如果日志目录下没有 segment 文件，就创建一个 activeSegment 作为起始，需要保证 Log 中至少有一个 LogSegment。
       if (logSegments.isEmpty) {
          addSegment(LogSegment.open(dir = dir,
            baseOffset = 0,
            config,
            time = time,
            fileAlreadyExists = false,
            initFileSize = this.initFileSize,
            preallocate = false))
       }
      // 目录被标记为“.deleted”时，将下一个偏移量设置为 0
      0
    }
  }


  // 更新日志段的末端偏移量，并同时更新其他相关元数据，例如高水位偏移量和恢复点偏移量等
  private def updateLogEndOffset(offset: Long): Unit = {
    // 构造一个新的 offset 元数据 LogOffsetMetadata 对象，
    // 参数分别是：给定的偏移量、当前活动日志段的基本偏移量 baseOffset、当前活动日志段的大小。
    nextOffsetMetadata = LogOffsetMetadata(offset, activeSegment.baseOffset, activeSegment.size)

    // Update the high watermark in case it has gotten ahead of the log end offset following a truncation
    // or if a new segment has been rolled and the offset metadata needs to be updated.
    // 如果已知的高水位偏移量大于等于给定的偏移量，则更新高水位元数据信息
    if (highWatermark >= offset) {
      updateHighWatermarkMetadata(nextOffsetMetadata)
    }

    // 如果恢复点偏移量大于给定的偏移量，则将恢复点更新为给定的偏移量。
    if (this.recoveryPoint > offset) {
      this.recoveryPoint = offset
    }
  }

  // 更新日志段的起始偏移量，并同时更新其他相关元数据，例如高水位偏移量和恢复点偏移量等
  private def updateLogStartOffset(offset: Long): Unit = {
    // 将日志段的起始偏移量设置为给定的偏移量。
    logStartOffset = offset

    // 如果已有的高水位偏移量小于给定的偏移量，则更新高水位。
    if (highWatermark < offset) {
      updateHighWatermark(offset)
    }

    // 如果恢复点偏移量小于给定的偏移量，则将恢复点更新为给定的偏移量
    if (this.recoveryPoint < offset) {
      this.recoveryPoint = offset
    }
  }

  /**
   * Recover the log segments and return the next offset after recovery.
   * This method does not need to convert IOException to KafkaStorageException because it is only called before all
   * logs are loaded.
   * @throws LogSegmentOffsetOverflowException if we encountered a legacy segment with offset overflow
   */
  private def recoverLog(): Long = {
    // if we have the clean shutdown marker, skip recovery
    // 如果不存在以 .kafka_cleanshutdown 结尾的文件，执行恢复
    if (!hasCleanShutdownFile) {
      // okay we need to actually recover this log
      // 获取到上次恢复点以外的所有 unflushed 日志段对象
      val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
      var truncated = false

      // 遍历 unflushed 日志段
      while (unflushed.hasNext && !truncated) {
        val segment = unflushed.next()
        info(s"Recovering unflushed segment ${segment.baseOffset}")
        val truncatedBytes =
          try {
            // 执行恢复日志段操作
            recoverSegment(segment, leaderEpochCache)
          } catch {
            case _: InvalidOffsetException =>
              val startOffset = segment.baseOffset
              warn("Found invalid offset during recovery. Deleting the corrupt segment and " +
                s"creating an empty one with starting offset $startOffset")
              segment.truncateTo(startOffset)
          }
        // 如果有无效的消息导致被截断的字节数不为0，直接删除剩余的日志段对象
        if (truncatedBytes > 0) {
          // we had an invalid message, delete all remaining log
          warn(s"Corruption found in segment ${segment.baseOffset}, truncating to offset ${segment.readNextOffset}")
          removeAndDeleteSegments(unflushed.toList,
            asyncDelete = true,
            reason = LogRecovery)
          truncated = true
        }
      }
    }

    // 上面这些都执行完之后，如果日志段集合不为空了
    if (logSegments.nonEmpty) {
      val logEndOffset = activeSegment.readNextOffset
      // 验证分区日志的LEO值不能小于Log Start Offset值，否则删除这些日志段对象
      if (logEndOffset < logStartOffset) {
        warn(s"Deleting all segments because logEndOffset ($logEndOffset) is smaller than logStartOffset ($logStartOffset). " +
          "This could happen if segment files were deleted from the file system.")
        removeAndDeleteSegments(logSegments,
          asyncDelete = true,
          reason = LogRecovery)
      }
    }

    // 上面这些都执行完之后，如果日志段集合为空了
    if (logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at logStartOffset
      // 至少创建一个新的日志段，以logStartOffset为日志段的起始位移，并加入日志段集合中
      addSegment(LogSegment.open(dir = dir,
        baseOffset = logStartOffset,
        config,
        time = time,
        fileAlreadyExists = false,
        initFileSize = this.initFileSize,
        preallocate = config.preallocate))
    }

    // 更新上次恢复点属性，并返回
    recoveryPoint = activeSegment.readNextOffset
    recoveryPoint
  }

  // Rebuild producer state until lastOffset. This method may be called from the recovery code path, and thus must be
  // free of all side-effects, i.e. it must not update any log-specific state.
  private def rebuildProducerState(lastOffset: Long,
                                   reloadFromCleanShutdown: Boolean,
                                   producerStateManager: ProducerStateManager): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    val messageFormatVersion = config.messageFormatVersion.recordVersion.value
    val segments = logSegments
    val offsetsToSnapshot =
      if (segments.nonEmpty) {
        val nextLatestSegmentBaseOffset = lowerSegment(segments.last.baseOffset).map(_.baseOffset)
        Seq(nextLatestSegmentBaseOffset, Some(segments.last.baseOffset), Some(lastOffset))
      } else {
        Seq(Some(lastOffset))
      }
    info(s"Loading producer state till offset $lastOffset with message format version $messageFormatVersion")

    // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
    // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
    // but we have to be careful not to assume too much in the presence of broker failures. The two most common
    // upgrade cases in which we expect to find no snapshots are the following:
    //
    // 1. The broker has been upgraded, but the topic is still on the old message format.
    // 2. The broker has been upgraded, the topic is on the new message format, and we had a clean shutdown.
    //
    // If we hit either of these cases, we skip producer state loading and write a new snapshot at the log end
    // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
    // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
    // from the first segment.
    if (messageFormatVersion < RecordBatch.MAGIC_VALUE_V2 ||
        (producerStateManager.latestSnapshotOffset.isEmpty && reloadFromCleanShutdown)) {
      // To avoid an expensive scan through all of the segments, we take empty snapshots from the start of the
      // last two segments and the last offset. This should avoid the full scan in the case that the log needs
      // truncation.
      offsetsToSnapshot.flatten.foreach { offset =>
        producerStateManager.updateMapEndOffset(offset)
        producerStateManager.takeSnapshot()
      }
    } else {
      val isEmptyBeforeTruncation = producerStateManager.isEmpty && producerStateManager.mapEndOffset >= lastOffset
      producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds())

      // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
      // offset (which would be the case on first startup) and there were active producers prior to truncation
      // (which could be the case if truncating after initial loading). If there weren't, then truncating
      // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
      // and we can skip the loading. This is an optimization for users which are not yet using
      // idempotent/transactional features yet.
      if (lastOffset > producerStateManager.mapEndOffset && !isEmptyBeforeTruncation) {
        val segmentOfLastOffset = floorLogSegment(lastOffset)

        logSegments(producerStateManager.mapEndOffset, lastOffset).foreach { segment =>
          val startOffset = Utils.max(segment.baseOffset, producerStateManager.mapEndOffset, logStartOffset)
          producerStateManager.updateMapEndOffset(startOffset)

          if (offsetsToSnapshot.contains(Some(segment.baseOffset)))
            producerStateManager.takeSnapshot()

          val maxPosition = if (segmentOfLastOffset.contains(segment)) {
            Option(segment.translateOffset(lastOffset))
              .map(_.position)
              .getOrElse(segment.size)
          } else {
            segment.size
          }

          val fetchDataInfo = segment.read(startOffset,
            maxSize = Int.MaxValue,
            maxPosition = maxPosition,
            minOneMessage = false)
          if (fetchDataInfo != null)
            loadProducersFromLog(producerStateManager, fetchDataInfo.records)
        }
      }
      producerStateManager.updateMapEndOffset(lastOffset)
      producerStateManager.takeSnapshot()
    }
  }

  private def loadProducerState(lastOffset: Long, reloadFromCleanShutdown: Boolean): Unit = lock synchronized {
    rebuildProducerState(lastOffset, reloadFromCleanShutdown, producerStateManager)
    maybeIncrementFirstUnstableOffset()
  }

  private def loadProducersFromLog(producerStateManager: ProducerStateManager, records: Records): Unit = {
    val loadedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    records.batches.forEach { batch =>
      if (batch.hasProducerId) {
        val maybeCompletedTxn = updateProducers(batch,
          loadedProducers,
          firstOffsetMetadata = None,
          origin = AppendOrigin.Replication)
        maybeCompletedTxn.foreach(completedTxns += _)
      }
    }
    loadedProducers.values.foreach(producerStateManager.update)
    completedTxns.foreach(producerStateManager.completeTxn)
  }

  private[log] def activeProducersWithLastSequence: Map[Long, Int] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      (producerId, producerIdEntry.lastSeq)
    }
  }

  private[log] def lastRecordsOfActiveProducers: Map[Long, LastRecord] = lock synchronized {
    producerStateManager.activeProducers.map { case (producerId, producerIdEntry) =>
      val lastDataOffset = if (producerIdEntry.lastDataOffset >= 0 ) Some(producerIdEntry.lastDataOffset) else None
      val lastRecord = LastRecord(lastDataOffset, producerIdEntry.producerEpoch)
      producerId -> lastRecord
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile: Boolean = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log.
   * The memory mapped buffer for index files of this log will be left open until the log is deleted.
   */
  def close(): Unit = {
    debug("Closing log")
    lock synchronized {
      checkIfMemoryMappedBufferClosed()
      producerExpireCheck.cancel(true)
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in dir ${dir.getParent}") {
        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
        // (the clean shutdown file is written after the logs are all closed).
        producerStateManager.takeSnapshot()
        logSegments.foreach(_.close())
      }
    }
  }

  /**
   * Rename the directory of the log
   *
   * @throws KafkaStorageException if rename fails
   */
  def renameDir(name: String): Unit = {
    lock synchronized {
      maybeHandleIOException(s"Error while renaming dir for $topicPartition in log dir ${dir.getParent}") {
        val renamedDir = new File(dir.getParent, name)
        Utils.atomicMoveWithFallback(dir.toPath, renamedDir.toPath)
        if (renamedDir != dir) {
          _dir = renamedDir
          _parentDir = renamedDir.getParent
          logSegments.foreach(_.updateParentDir(renamedDir))
          producerStateManager.logDir = dir
          // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
          // the checkpoint file in renamed log directory
          initializeLeaderEpochCache()
        }
      }
    }
  }

  /**
   * Close file handlers used by log but don't write to disk. This is called if the log directory is offline
   */
  def closeHandlers(): Unit = {
    debug("Closing handlers")
    lock synchronized {
      logSegments.foreach(_.closeHandlers())
      isMemoryMappedBufferClosed = true
    }
  }

  /**
   * Append this message set to the active segment of the log, assigning offsets and Partition Leader Epochs
   *
   * @param records The records to append
   * @param origin Declares the origin of the append which affects required validations
   * @param interBrokerProtocolVersion Inter-broker message protocol version
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  // 用来写 Leader 副本的，底层都调用了 append 方法。
  def appendAsLeader(records: MemoryRecords,
                     leaderEpoch: Int,
                     origin: AppendOrigin = AppendOrigin.Client,
                     interBrokerProtocolVersion: ApiVersion = ApiVersion.latestVersion): LogAppendInfo = {
    // assignOffsets = true 表示是 Leader 副本追加需要分配 offset
    append(records, origin, interBrokerProtocolVersion, assignOffsets = true, leaderEpoch, ignoreRecordSize = false)
  }

  /**
   * Append this message set to the active segment of the log without assigning offsets or Partition Leader Epochs
   *
   * @param records The records to append
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  // 作为 Follower 分区向 Follower分区追加日志，即副本数据同步的，底层调用了 append 方法。
  def appendAsFollower(records: MemoryRecords): LogAppendInfo = {
    append(records,
      origin = AppendOrigin.Replication,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      assignOffsets = false,
      leaderEpoch = -1,
      // disable to check the validation of record size since the record is already accepted by leader.
      ignoreRecordSize = true)
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param records The log records to append
   * @param origin Declares the origin of the append which affects required validations
   * @param interBrokerProtocolVersion Inter-broker message protocol version
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * @param leaderEpoch The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
   * @param ignoreRecordSize true to skip validation of record size.
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @throws OffsetsOutOfOrderException If out of order offsets found in 'records'
   * @throws UnexpectedAppendOffsetException If the first or last offset in append is less than next offset
   * @return Information about the appended messages including the first and last offset.
   */
  // 注意：assignOffsets 是否需要分配位移，leader 副本需要，follwer 副本则不需要
  private def append(records: MemoryRecords,
                     origin: AppendOrigin,
                     interBrokerProtocolVersion: ApiVersion,
                     assignOffsets: Boolean,
                     leaderEpoch: Int,
                     ignoreRecordSize: Boolean): LogAppendInfo = {
    maybeHandleIOException(s"Error while appending records to $topicPartition in dir ${dir.getParent}") {
      // 判断消息格式是否正确，并返回校验结果, 生成对应的 LogAppendInfo 对象
      val appendInfo = analyzeAndValidateRecords(records, origin, ignoreRecordSize)

      // return if we have no valid messages or if this is a duplicate of the last appended entry
      // 如果没有一条消息格式正确，直接返回
      if (appendInfo.shallowCount == 0)
        return appendInfo

      // trim any invalid bytes or partial messages before appending it to the on-disk log
      // 在将追加到磁盘上的日志分段之前，进行消息格式规整，修剪任何无效的字节或部分消息
      var validRecords = trimInvalidBytes(records, appendInfo)

      // they are valid, insert them in the log
      // 加锁处理消息，避免多个生产者同时写入该 Log 文件，如果需要进行记录偏移的分配，通过「消息验证器」的辅助，分配消息记录的偏移
      lock synchronized {
        // 确保 Log 对象未关闭
        checkIfMemoryMappedBufferClosed()
        // 如果需要给消息分配 offset的话，leader 副本需要，follwer 副本则不需要
        if (assignOffsets) {
          // assign offsets to the message set
          // 计算第一条消息的 offset：使用当前 LEO 值作为待写入消息集合中第一条消息的位移值
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          appendInfo.firstOffset = Some(offset.value)
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            //  验证消息记录并分配偏移，给每一条消息设置 offset，并且找出 maxTimestamp 以及 maxTimestamp 对应的 offset
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
              topicPartition,
              offset,
              time,
              now,
              appendInfo.sourceCodec,
              appendInfo.targetCodec,
              config.compact,
              config.messageFormatVersion.recordVersion.value,
              config.messageTimestampType,
              config.messageTimestampDifferenceMaxMs,
              leaderEpoch,
              origin,
              interBrokerProtocolVersion,
              brokerTopicStats)
          } catch {
            case e: IOException =>
              throw new KafkaException(s"Error validating messages while appending to log $name", e)
          }
          // 获取有效的记录，然后根据这些记录设置响应的返回内容
          validRecords = validateAndOffsetAssignResult.validatedRecords
          // 消息的最大时间戳和配置的 messageTimestampType 有关系。当前获取消息maxTimestamp时间戳的方式有两种。
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          // 根据消息的 timestamp 来设置时间戳
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          appendInfo.recordConversionStats = validateAndOffsetAssignResult.recordConversionStats
          // 根据消息的写入时间来设置时间戳即当前时间
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          // 由于前面的操作可能导致消息压缩格式改变以及消息格式改变，再次验证消息大小不超限，
          // 即检查当前的每条消息大小是否超过 maxMessageSize 的配置大小
          if (!ignoreRecordSize && validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (batch <- validRecords.batches.asScala) {
              if (batch.sizeInBytes > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException(s"Message batch size is ${batch.sizeInBytes} bytes in append to" +
                  s"partition $topicPartition which exceeds the maximum configured size of ${config.maxMessageSize}.")
              }
            }
          }
        } else {
          // we are taking the offsets we are given
          // 接收客户端分配的偏移
          if (!appendInfo.offsetsMonotonic)
            throw new OffsetsOutOfOrderException(s"Out of order offsets found in append to $topicPartition: " +
                                                 records.records.asScala.map(_.offset))

          // 如果第一批消息的第一个或最后一个偏移值小于下一个消息的偏移值，则抛出异常
          if (appendInfo.firstOrLastOffsetOfFirstBatch < nextOffsetMetadata.messageOffset) {
            // we may still be able to recover if the log is empty
            // one example: fetching from log start offset on the leader which is not batch aligned,
            // which may happen as a result of AdminClient#deleteRecords()
            val firstOffset = appendInfo.firstOffset match {
              case Some(offset) => offset
              case None => records.batches.asScala.head.baseOffset()
            }

            val firstOrLast = if (appendInfo.firstOffset.isDefined) "First offset" else "Last offset of the first batch"
            throw new UnexpectedAppendOffsetException(
              s"Unexpected offset in append to $topicPartition. $firstOrLast " +
              s"${appendInfo.firstOrLastOffsetOfFirstBatch} is less than the next offset ${nextOffsetMetadata.messageOffset}. " +
              s"First 10 offsets in append: ${records.records.asScala.take(10).map(_.offset)}, last offset in" +
              s" append: ${appendInfo.lastOffset}. Log start offset = $logStartOffset",
              firstOffset, appendInfo.lastOffset)
          }
        }

        // update the epoch cache with the epoch stamped onto the message by the leader
        // 将每一条消息记录的 Leader Epoch 更新到 Leader Epoch Cache 缓存中
        validRecords.batches.forEach { batch =>
          if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
            maybeAssignEpochStartOffset(batch.partitionLeaderEpoch, batch.baseOffset)
          } else {
            // In partial upgrade scenarios, we may get a temporary regression to the message format. In
            // order to ensure the safety of leader election, we clear the epoch cache so that we revert
            // to truncation by high watermark after the next leader election.
            // 确保 Leader Epoch 一致
            leaderEpochCache.filter(_.nonEmpty).foreach { cache =>
              warn(s"Clearing leader epoch cache after unexpected append with message format v${batch.magic}")
              cache.clearAndFlush()
            }
          }
        }

        // check messages set size may be exceed config.segmentSize
        // 确保消息大小是否超过 config.segmentSize 的设置。即 1G 大小
        // config.segmentSize = kafka.server.Defaults.LogSegmentBytes = 1 * 1024 * 1024 * 1024
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException(s"Message batch size is ${validRecords.sizeInBytes} bytes in append " +
            s"to partition $topicPartition, which exceeds the maximum configured segment size of ${config.segmentSize}.")
        }

        // maybe roll the log if this segment is full
        // 写入日志分段，检查是否需要切换到新的日志段，如果当前日志分段已满，则创建新的日志段，替换当前写入的旧日志段。
        // 当前日志段剩余容量可能无法容纳新消息集合，因此有必要创建一个新的日志段来保存待写入的所有消息
        val segment = maybeRoll(validRecords.sizeInBytes, appendInfo)

        // 构建日志分段元数据（即写入的消息记录的元数据）
        val logOffsetMetadata = LogOffsetMetadata(
          messageOffset = appendInfo.firstOrLastOffsetOfFirstBatch,
          segmentBaseOffset = segment.baseOffset,
          relativePositionInSegment = segment.size)

        // now that we have valid records, offsets assigned, and timestamps updated, we need to
        // validate the idempotent/transactional state of the producers and collect some metadata
        // 验证并更新 Producer 状态，主要关注事务 ID 和幂等 ID 的状态
        val (updatedProducers, completedTxns, maybeDuplicate) = analyzeAndValidateProducerState(
          logOffsetMetadata, validRecords, origin)

        // 验证是否存在重复消息记录
        maybeDuplicate.foreach { duplicate =>
          appendInfo.firstOffset = Some(duplicate.firstOffset)
          appendInfo.lastOffset = duplicate.lastOffset
          appendInfo.logAppendTime = duplicate.timestamp
          appendInfo.logStartOffset = logStartOffset
          return appendInfo
        }

        // 执行真正的消息写入操作，主要调用日志段对象的 append 方法实现
        segment.append(largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // Increment the log end offset. We do this immediately after the append because a
        // write to the transaction index below may fail and we want to ensure that the offsets
        // of future appends still grow monotonically. The resulting transaction index inconsistency
        // will be cleaned up after the log directory is recovered. Note that the end offset of the
        // ProducerStateManager will not be updated and the last stable offset will not advance
        // if the append to the transaction index fails.
        // 更新 Log#nextOffsetMetadata 即下一条消息的偏移量
        updateLogEndOffset(appendInfo.lastOffset + 1)

        // update the producer state
        // 更新 Producer 状态
        for (producerAppendInfo <- updatedProducers.values) {
          producerStateManager.update(producerAppendInfo)
        }

        // update the transaction index with the true last stable offset. The last offset visible
        // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
        // 更新事务索引状态
        for (completedTxn <- completedTxns) {
          val lastStableOffset = producerStateManager.lastStableOffset(completedTxn)
          // 会调用 logSegment#updateTxnIndex 方法
          segment.updateTxnIndex(completedTxn, lastStableOffset)
          producerStateManager.completeTxn(completedTxn)
        }

        // always update the last producer id map offset so that the snapshot reflects the current offset
        // even if there isn't any idempotent data being written
        // 总是更新最后的 Producer ID 映射偏移量，以便快照反映当前偏移量状态
        producerStateManager.updateMapEndOffset(appendInfo.lastOffset + 1)

        // update the first unstable offset (which is used to compute LSO)
        // 更新不稳定的首个偏移量（用于计算 LSO）
        maybeIncrementFirstUnstableOffset()

        trace(s"Appended message set with last offset: ${appendInfo.lastOffset}, " +
          s"first offset: ${appendInfo.firstOffset}, " +
          s"next offset: ${nextOffsetMetadata.messageOffset}, " +
          s"and messages: $validRecords")

        // 如果未刷新的消息超过刷新间隔，则执行强制刷新
        if (unflushedMessages >= config.flushInterval)
          flush()

        // 返回追加的消息
        appendInfo
      }
    }
  }

  def maybeAssignEpochStartOffset(leaderEpoch: Int, startOffset: Long): Unit = {
    leaderEpochCache.foreach { cache =>
      cache.assign(leaderEpoch, startOffset)
    }
  }

  def latestEpoch: Option[Int] = leaderEpochCache.flatMap(_.latestEpoch)

  def endOffsetForEpoch(leaderEpoch: Int): Option[OffsetAndEpoch] = {
    leaderEpochCache.flatMap { cache =>
      val (foundEpoch, foundOffset) = cache.endOffsetFor(leaderEpoch)
      if (foundOffset == EpochEndOffset.UNDEFINED_EPOCH_OFFSET)
        None
      else
        Some(OffsetAndEpoch(foundOffset, foundEpoch))
    }
  }

  private def maybeIncrementFirstUnstableOffset(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()

    val updatedFirstStableOffset = producerStateManager.firstUnstableOffset match {
      case Some(logOffsetMetadata) if logOffsetMetadata.messageOffsetOnly || logOffsetMetadata.messageOffset < logStartOffset =>
        val offset = math.max(logOffsetMetadata.messageOffset, logStartOffset)
        Some(convertToOffsetMetadataOrThrow(offset))
      case other => other
    }

    // 更新 Log#firstUnstableOffsetMetadata，它的取值规则：取生产者管理状态中的第一个不稳定偏移量的
    // 偏移量 logOffsetMetadata.messageOffset 和日志的基准偏移量 logStartOffset 之间较大的那个。
    if (updatedFirstStableOffset != this.firstUnstableOffsetMetadata) {
      debug(s"First unstable offset updated to $updatedFirstStableOffset")
      this.firstUnstableOffsetMetadata = updatedFirstStableOffset
    }
  }

  /**
   * Increment the log start offset if the provided offset is larger.
   */
  def maybeIncrementLogStartOffset(newLogStartOffset: Long, reason: LogStartOffsetIncrementReason): Unit = {
    // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
    // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
    // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
    maybeHandleIOException(s"Exception while increasing log start offset for $topicPartition to $newLogStartOffset in dir ${dir.getParent}") {
      lock synchronized {
        if (newLogStartOffset > highWatermark)
          throw new OffsetOutOfRangeException(s"Cannot increment the log start offset to $newLogStartOffset of partition $topicPartition " +
            s"since it is larger than the high watermark $highWatermark")

        checkIfMemoryMappedBufferClosed()
        if (newLogStartOffset > logStartOffset) {
          updateLogStartOffset(newLogStartOffset)
          info(s"Incremented log start offset to $newLogStartOffset due to $reason")
          leaderEpochCache.foreach(_.truncateFromStart(logStartOffset))
          producerStateManager.truncateHead(newLogStartOffset)
          maybeIncrementFirstUnstableOffset()
        }
      }
    }
  }

  private def analyzeAndValidateProducerState(appendOffsetMetadata: LogOffsetMetadata,
                                              records: MemoryRecords,
                                              origin: AppendOrigin):
  (mutable.Map[Long, ProducerAppendInfo], List[CompletedTxn], Option[BatchMetadata]) = {
    val updatedProducers = mutable.Map.empty[Long, ProducerAppendInfo]
    val completedTxns = ListBuffer.empty[CompletedTxn]
    var relativePositionInSegment = appendOffsetMetadata.relativePositionInSegment

    // 遍历所有的消息批次进行处理
    for (batch <- records.batches.asScala) {
      if (batch.hasProducerId) {
        val maybeLastEntry = producerStateManager.lastEntry(batch.producerId)

        // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
        // If we find a duplicate, we return the metadata of the appended batch to the client.
        if (origin == AppendOrigin.Client) {
          // 获取该批次的生产者元数据 producerStateEntry
          // 调用 findDuplicateBatch 来查找重复的消息批次
          maybeLastEntry.flatMap(_.findDuplicateBatch(batch)).foreach { duplicate =>
            return (updatedProducers, completedTxns.toList, Some(duplicate))
          }
        }

        // We cache offset metadata for the start of each transaction. This allows us to
        // compute the last stable offset without relying on additional index lookups.
        val firstOffsetMetadata = if (batch.isTransactional)
          Some(LogOffsetMetadata(batch.baseOffset, appendOffsetMetadata.segmentBaseOffset, relativePositionInSegment))
        else
          None

        // 调用 updateProducers 方法，以消息生产者为维度，将消息的事务信息分组汇总存储到不同的 ProducerAppendInfo 中。
        val maybeCompletedTxn = updateProducers(batch, updatedProducers, firstOffsetMetadata, origin)
        // 将上一步得到的 completedTxn 实例添加到 completedTxns 集合中。
        maybeCompletedTxn.foreach(completedTxns += _)
      }

      relativePositionInSegment += batch.sizeInBytes
    }
    (updatedProducers, completedTxns.toList, None)
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid (if ignoreRecordSize is false)
   * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other.
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */
  private def analyzeAndValidateRecords(records: MemoryRecords,
                                        origin: AppendOrigin,
                                        ignoreRecordSize: Boolean): LogAppendInfo = {
    var shallowMessageCount = 0
    var validBytesCount = 0
    var firstOffset: Option[Long] = None
    var lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    var readFirstMessage = false
    var lastOffsetOfFirstBatch = -1L

    // 遍历 MemoryRecords 内的所有的batch。浅层遍历
    for (batch <- records.batches.asScala) {
      // we only validate V2 and higher to avoid potential compatibility issues with older clients
      // 消息格式Version 2 的消息批次，起始位移值必须从 0 开始
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2 && origin == AppendOrigin.Client && batch.baseOffset != 0)
        throw new InvalidRecordException(s"The baseOffset of the record batch in the append to $topicPartition should " +
          s"be 0, but it is ${batch.baseOffset}")

      // update the first offset if on the first message. For magic versions older than 2, we use the last offset
      // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
      // For magic version 2, we can get the first offset directly from the batch header.
      // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
      // case, validation will be more lenient.
      // Also indicate whether we have the accurate first offset or not
      if (!readFirstMessage) {
        if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
          firstOffset = Some(batch.baseOffset)
        lastOffsetOfFirstBatch = batch.lastOffset
        readFirstMessage = true
      }

      // check that offsets are monotonically increasing
      // 2、一旦出现当前 lastOffset 不小于下一个 batch 的 lastOffset，
      // 说明上一个 batch 中有消息的位移值大于后面 batch 的消息，这违反了位移值单调递增性
      if (lastOffset >= batch.lastOffset)
        monotonic = false

      // update the last offset seen
      // 使用当前 batch 最后一条消息的位移值去更新 lastOffset
      lastOffset = batch.lastOffset

      // Check if the message sizes are valid.
      // 检查消息批次总字节数大小是否超限，即是否大于 Broker 端参数 max.message.bytes 值
      val batchSize = batch.sizeInBytes
      if (!ignoreRecordSize && batchSize > config.maxMessageSize) {
        brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        brokerTopicStats.allTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException(s"The record batch size in the append to $topicPartition is $batchSize bytes " +
          s"which exceeds the maximum configured value of ${config.maxMessageSize}.")
      }

      // check the validity of the message by checking CRC
      // 执行消息批次校验，包括格式是否正确以及CRC校验
      if (!batch.isValid) {
        brokerTopicStats.allTopicsStats.invalidMessageCrcRecordsPerSec.mark()
        throw new CorruptRecordException(s"Record is corrupt (stored crc = ${batch.checksum()}) in topic partition $topicPartition.")
      }

      // 更新maxTimestamp字段和offsetOfMaxTimestamp
      if (batch.maxTimestamp > maxTimestamp) {
        maxTimestamp = batch.maxTimestamp
        offsetOfMaxTimestamp = lastOffset
      }

      // 累加消息批次计数器以及有效字节数，更新shallowMessageCount字段
      shallowMessageCount += 1
      validBytesCount += batchSize

      // 从消息批次中获取压缩器类型
      val messageCodec = CompressionCodec.getCompressionCodec(batch.compressionType.id)
      if (messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    // 获取 Broker 端设置的压缩器类型，即 Broker 端参数 compression.type 值。
    // 该参数默认值是 producer，表示 sourceCodec 用的什么压缩器，targetCodec 就用什么
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)
    // 最后生成LogAppendInfo对象并返回
    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, RecordBatch.NO_TIMESTAMP, logStartOffset,
      RecordConversionStats.EMPTY, sourceCodec, targetCodec, shallowMessageCount, validBytesCount, monotonic, lastOffsetOfFirstBatch)
  }

  private def updateProducers(batch: RecordBatch,
                              producers: mutable.Map[Long, ProducerAppendInfo],
                              firstOffsetMetadata: Option[LogOffsetMetadata],
                              origin: AppendOrigin): Option[CompletedTxn] = {
    // 获取生产者id
    val producerId = batch.producerId
    // 从 updateProducers 集合中查找该消息批次生产者对应的 ProducerAppendInfo 实例，如果不存在则创建
    val appendInfo = producers.getOrElseUpdate(producerId, producerStateManager.prepareUpdate(producerId, origin))
    // 调用 ProducerAppendInfo#append 方法将该消息批次的事务消息添加到 ProducerAppendInfo 中
    appendInfo.append(batch, firstOffsetMetadata)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param records The records to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    val validBytes = info.validBytes
    if (validBytes < 0)
      throw new CorruptRecordException(s"Cannot append record batch with illegal length $validBytes to " +
        s"log for $topicPartition. A possible cause is a corrupted produce request.")
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // trim invalid bytes
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  private def emptyFetchDataInfo(fetchOffsetMetadata: LogOffsetMetadata,
                                 includeAbortedTxns: Boolean): FetchDataInfo = {
    val abortedTransactions =
      if (includeAbortedTxns) Some(List.empty[AbortedTransaction])
      else None
    FetchDataInfo(fetchOffsetMetadata,
      MemoryRecords.EMPTY,
      firstEntryIncomplete = false,
      abortedTransactions = abortedTransactions)
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param isolation The fetch isolation, which controls the maximum offset we are allowed to read
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  // 先找到 segment（包含log index timeindex三个文件），再从segment里面拉取消息。
  def read(startOffset: Long, // 从 Log 对象的哪个位移值开始读消息
           maxLength: Int, // 最多能读取多少字节。
           isolation: FetchIsolation, //设置读取隔离级别，主要控制能够读取的最大位移值，多用于 Kafka 事务
           minOneMessage: Boolean): FetchDataInfo = { // 是否允许至少读一条消息。
    // 设想如果消息很大，超过了maxLength，正常情况下read方法永远不会返回任何消息。
    // 但如果设置了该参数为 true，read方法就保证至少能够返回一条消息。

    maybeHandleIOException(s"Exception while reading from $topicPartition in dir ${dir.getParent}") {
      trace(s"Reading maximum $maxLength bytes at offset $startOffset from log with " +
        s"total length $size bytes")

      val includeAbortedTxns = isolation == FetchTxnCommitted

      // Because we don't use the lock for reading, the synchronization is a little bit tricky.
      // We create the local variables to avoid race conditions with updates to the log.

      // 这里读取消息时我们没有使用 Monitor 锁同步机制，用本地变量的方式把LEO对象保存起来，避免争用（race condition）
      // 获取下一个偏移元数据信息
      val endOffsetMetadata = nextOffsetMetadata
      // 获取下一个偏移量
      val endOffset = endOffsetMetadata.messageOffset
      // 获取指定偏移量所对应的日志段，找到startOffset值所在的日志段对象。注意要使用floorEntry方法
      var segmentEntry = segments.floorEntry(startOffset)

      // return error on attempt to read beyond the log end offset or read below log start offset
      // 1、判断要读取的起始偏移量是否越界，满足以下条件之一将被视为消息越界，即你要读取的消息不在该Log对象中，
      //    则返回 OffsetOutOfRangeException 异常：

      // 1. 要读取的消息位移超过了LEO值
      // 2. 没找到对应的日志段对象
      // 3. 要读取的消息在Log Start Offset之下，同样是对外不可见的消息
      if (startOffset > endOffset || segmentEntry == null || startOffset < logStartOffset)
        throw new OffsetOutOfRangeException(s"Received request for offset $startOffset for partition $topicPartition, " +
          s"but we only have log segments in the range $logStartOffset to $endOffset.")

      // 2、根据读取隔离级别设置，得出能读到最大offset的值，这里分三种情况。
      val maxOffsetMetadata = isolation match {
        // 1.Follower副本消费者能够看到[Log Start Offset，LEO)之间的消息
        case FetchLogEnd => endOffsetMetadata
        // 2.普通消费者能够看到[Log Start Offset, 高水位值)之间的消息
        case FetchHighWatermark => fetchHighWatermarkMetadata
        // 3.事务型消费者只能看到[Log Start Offset, Log Stable Offset]之间的消息。
        //   Log Stable Offset(LSO)是比LEO值小的位移值，主要用在 Kafka事务消息。
        case FetchTxnCommitted => fetchLastStableOffsetMetadata
      }

      // 如果要读取的起始位置超过了能读取的最大偏移量位置，则返回空的消息集合，因为没法读取任何消息
      if (startOffset == maxOffsetMetadata.messageOffset) {
        return emptyFetchDataInfo(maxOffsetMetadata, includeAbortedTxns)
      } else if (startOffset > maxOffsetMetadata.messageOffset) {
        val startOffsetMetadata = convertToOffsetMetadataOrThrow(startOffset)
        return emptyFetchDataInfo(startOffsetMetadata, includeAbortedTxns)
      }

      // Do the read on the segment with a base offset less than the target offset
      // but if that segment doesn't contain any messages with an offset greater than that
      // continue to read from successive segments until we get some messages or we reach the end of the log

      // 从以前的日志段中读取数据
      // 如果日志段中没有任何消息的偏移量大于当前的偏移量，就继续在后续日志段中读取数据，
      // 直到最后一次读取或者遇到日志末尾

      // 开始遍历日志段对象，直到读出消息来或者读到日志末尾
      while (segmentEntry != null) {
        val segment = segmentEntry.getValue

        // 计算最大位置
        // maxOffsetMetadata.relativePositionInSegment: 保存该位移值所在日志段的物理磁盘位置
        val maxPosition = {
          // Use the max offset position if it is on this segment; otherwise, the segment size is the limit.
          // 如果当前 segment 的 baseOffset 等于 maxOffsetMetadata 的最大值，则 maxPosition 就是日志段的物理磁盘位置
          if (maxOffsetMetadata.segmentBaseOffset == segment.baseOffset) {
            maxOffsetMetadata.relativePositionInSegment
          } else {
            // 否则，限制长度为当前日志段的大小
            segment.size
          }
        }

        // 调用日志段对象的 read 方法执行真正的读取消息操作
        val fetchInfo = segment.read(startOffset, maxLength, maxPosition, minOneMessage)
        // 如果读取数据为空，则读取下一个日志段中的数据
        if (fetchInfo == null) {
          segmentEntry = segments.higherEntry(segmentEntry.getKey)
        } else {
          return if (includeAbortedTxns)
            addAbortedTransactions(startOffset, segmentEntry, fetchInfo)
          else
            fetchInfo
        }
      }

      // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
      // this can happen when all messages with offset larger than start offsets have been deleted.
      // In this case, we will return the empty set with log end offset metadata
      // 读取数据为空，但指定的偏移量在日志的合法范围内，说明再次读取后已经读取到了日志的末尾，
      // 此时如果没有数据被读取，说明所有偏移量都已被删除，因此返回空日志记录
      FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
    }
  }

  private[log] def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long): List[AbortedTxn] = {
    val segmentEntry = segments.floorEntry(startOffset)
    val allAbortedTxns = ListBuffer.empty[AbortedTxn]
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = allAbortedTxns ++= abortedTxns
    collectAbortedTransactions(logStartOffset, upperBoundOffset, segmentEntry, accumulator)
    allAbortedTxns.toList
  }

  // 添加未提交的事务到 FetchDataInfo 中
  // startOffset 当前拉取的起始偏移量
  // segmentEntry 当前段的JEntry对象，包含段的元数据和实例对象
  // fetchInfo 当前拉取的数据信息
  private def addAbortedTransactions(startOffset: Long, segmentEntry: JEntry[JLong, LogSegment],
                                     fetchInfo: FetchDataInfo): FetchDataInfo = {
    // 获取当前拉取的消息大小
    val fetchSize = fetchInfo.records.sizeInBytes
    // 获取拉取的起始偏移量的位置信息
    val startOffsetPosition = OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
      fetchInfo.fetchOffsetMetadata.relativePositionInSegment)
    // 计算拉取数据的上界偏移量，不得超过段的末尾。如果超过了末尾，则使用下一个段的基准偏移量作为拉取的上界偏移量
    val upperBoundOffset = segmentEntry.getValue.fetchUpperBoundOffset(startOffsetPosition, fetchSize).getOrElse {
      val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
      if (nextSegmentEntry != null)
        nextSegmentEntry.getValue.baseOffset
      else
        logEndOffset
    }

    // 创建一个 ListBuffer，用来存储未提交的事务
    val abortedTransactions = ListBuffer.empty[AbortedTransaction]
    // 定义一个累加器函数，将 AbortedTxn 转换为 AbortedTransaction 并添加到 abortedTransactions 中
    def accumulator(abortedTxns: List[AbortedTxn]): Unit = abortedTransactions ++= abortedTxns.map(_.asAbortedTransaction)
    // 收集起始偏移量和上界偏移量之间的未提交事务
    collectAbortedTransactions(startOffset, upperBoundOffset, segmentEntry, accumulator)

    // 构造并返回包含未提交事务的FetchDataInfo对象
    FetchDataInfo(fetchOffsetMetadata = fetchInfo.fetchOffsetMetadata,
      records = fetchInfo.records,
      firstEntryIncomplete = fetchInfo.firstEntryIncomplete,
      abortedTransactions = Some(abortedTransactions.toList))
  }

  private def collectAbortedTransactions(startOffset: Long, upperBoundOffset: Long,
                                         startingSegmentEntry: JEntry[JLong, LogSegment],
                                         accumulator: List[AbortedTxn] => Unit): Unit = {
    var segmentEntry = startingSegmentEntry
    while (segmentEntry != null) {
      val searchResult = segmentEntry.getValue.collectAbortedTxns(startOffset, upperBoundOffset)
      accumulator(searchResult.abortedTransactions)
      if (searchResult.isComplete)
        return
      segmentEntry = segments.higherEntry(segmentEntry.getKey)
    }
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   */
  def fetchOffsetByTimestamp(targetTimestamp: Long): Option[TimestampAndOffset] = {
    maybeHandleIOException(s"Error while fetching offset by timestamp for $topicPartition in dir ${dir.getParent}") {
      debug(s"Searching offset for timestamp $targetTimestamp")

      if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
        throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

      // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
      // constant time access while being safe to use with concurrent collections unlike `toArray`.
      val segmentsCopy = logSegments.toBuffer
      // For the earliest and latest, we do not need to return the timestamp.
      if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP) {
        // The first cached epoch usually corresponds to the log start offset, but we have to verify this since
        // it may not be true following a message format version bump as the epoch will not be available for
        // log entries written in the older format.
        val earliestEpochEntry = leaderEpochCache.flatMap(_.earliestEntry)
        val epochOpt = earliestEpochEntry match {
          case Some(entry) if entry.startOffset <= logStartOffset => Optional.of[Integer](entry.epoch)
          case _ => Optional.empty[Integer]()
        }
        return Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logStartOffset, epochOpt))
      } else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP) {
        val latestEpochOpt = leaderEpochCache.flatMap(_.latestEpoch).map(_.asInstanceOf[Integer])
        val epochOptional = Optional.ofNullable(latestEpochOpt.orNull)
        return Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logEndOffset, epochOptional))
      }

      // We need to search the first segment whose largest timestamp is >= the target timestamp if there is one.
      val targetSeg = segmentsCopy.find(_.largestTimestamp >= targetTimestamp)
      targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp, logStartOffset))
    }
  }

  def legacyFetchOffsetsBefore(timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val segments = logSegments.toBuffer
    val lastSegmentHasSize = segments.last.size > 0

    val offsetTimeArray =
      if (lastSegmentHasSize)
        new Array[(Long, Long)](segments.length + 1)
      else
        new Array[(Long, Long)](segments.length)

    for (i <- segments.indices)
      offsetTimeArray(i) = (math.max(segments(i).baseOffset, logStartOffset), segments(i).lastModified)
    if (lastSegmentHasSize)
      offsetTimeArray(segments.length) = (logEndOffset, time.milliseconds)

    var startIndex = -1
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        startIndex = offsetTimeArray.length - 1
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        startIndex = 0
      case _ =>
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -= 1
        }
    }

    val retSize = maxNumOffsets.min(startIndex + 1)
    val ret = new Array[Long](retSize)
    for (j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(-_)
  }

  /**
    * Given a message offset, find its corresponding offset metadata in the log.
    * If the message offset is out of range, throw an OffsetOutOfRangeException
    */
  // 将指定的偏移量转换为对应的 offset 元数据
  private def convertToOffsetMetadataOrThrow(offset: Long): LogOffsetMetadata = {
    // 从偏移量处读取消息数据，并从读取的数据中提取出 offset 元数据
    val fetchDataInfo = read(offset,
      maxLength = 1, // maxLength 参数表示最多读取的字节数
      isolation = FetchLogEnd,  // isolation 参数表示读取数据时应如何隔离消息
      minOneMessage = false) // minOneMessage 参数表示是否最好检索出至少一个消息，如果没有则抛出异常

    // 从读取到的数据中获取 offset 元数据，并返回该值。
    fetchDataInfo.fetchOffsetMetadata
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean,
                                reason: SegmentDeletionReason): Int = {
    lock synchronized {
      // 使用传入的函数计算哪些日志段对象能够被删除
      val deletable = deletableSegments(predicate)
      // 如果存在可删除的日志段对象
      if (deletable.nonEmpty)
        // 调用 deleteSegments 方法删除这些日志段。
        deleteSegments(deletable, reason)
      else
        0
    }
  }

  private def deleteSegments(deletable: Iterable[LogSegment], reason: SegmentDeletionReason): Int = {
    maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        // 不允许删除所有日志段对象。如果一定要做，先创建出一个新的来，然后再把前面N个删掉
        if (segments.size == numToDelete)
          roll()
        lock synchronized {
          // 确保 Log 对象没有被关闭
          checkIfMemoryMappedBufferClosed()
          // remove the segments for lookups
          // 删除给定的日志段对象以及底层的物理文件
          removeAndDeleteSegments(deletable, asyncDelete = true, reason)
          // 尝试更新日志的Log Start Offset值
          maybeIncrementLogStartOffset(segments.firstEntry.getValue.baseOffset, SegmentDeletion)
        }
      }
      // 返回删除的日志段
      numToDelete
    }
  }

  /**
   * Find segments starting from the oldest until the user-supplied predicate is false or the segment
   * containing the current high watermark is reached. We do not delete segments with offsets at or beyond
   * the high watermark to ensure that the log start offset can never exceed it. If the high watermark
   * has not yet been initialized, no segments are eligible for deletion.
   *
   * A final segment that is empty will never be returned (since we would just end up re-creating it).
   *
   * @param predicate A function that takes in a candidate log segment and the next higher segment
   *                  (if there is one) and returns true iff it is deletable
   * @return the segments ready to be deleted
   */
  private def deletableSegments(predicate: (LogSegment, Option[LogSegment]) => Boolean): Iterable[LogSegment] = {
    // 如果当前压根就没有任何日志段对象，直接返回
    if (segments.isEmpty) {
      Seq.empty
    } else {
      val deletable = ArrayBuffer.empty[LogSegment]
      // 获取第一个日志段对象
      var segmentEntry = segments.firstEntry
      // 从具有最小起始位移值的日志段对象开始遍历，直到满足以下条件之一便停止遍历：
      // 1. 扫描到包含Log对象高水位值所在的日志段对象
      // 2. 测定条件函数predicate = false
      // 3. 最新的日志段对象不包含任何消息
      // 最新日志段对象是 segments 中Key值最大对应的那个日志段，也就是我们常说的 Active Segment。
      // 完全为空的 Active Segment 如果被允许删除，后面还要重建它，故代码这里不允许删除大小为空的Active Segment。
      while (segmentEntry != null) {
        // 获取日志段数据
        val segment = segmentEntry.getValue
        // 第一个起始位移值≥给定 Key 值的日志段对象
        val nextSegmentEntry = segments.higherEntry(segmentEntry.getKey)
        val (nextSegment, upperBoundOffset, isLastSegmentAndEmpty) = if (nextSegmentEntry != null)
          (nextSegmentEntry.getValue, nextSegmentEntry.getValue.baseOffset, false)
        else
          (null, logEndOffset, segment.size == 0)

        // 必须满足这三个条件才能删除
        if (highWatermark >= upperBoundOffset && predicate(segment, Option(nextSegment)) && !isLastSegmentAndEmpty) {
          deletable += segment
          segmentEntry = nextSegmentEntry
        } else {
          segmentEntry = null
        }
      }
      deletable
    }
  }

  /**
   * If topic deletion is enabled, delete any log segments that have either expired due to time based retention
   * or because the log size is > retentionSize.
   *
   * Whether or not deletion is enabled, delete any log segments that are before the log start offset
   */
  def deleteOldSegments(): Int = {
    if (config.delete) {
      // 基于「时间维度」+ 基于「空间维度」+ 基于「LogStartOffset 维度」
      deleteRetentionMsBreachedSegments() + deleteRetentionSizeBreachedSegments() + deleteLogStartOffsetBreachedSegments()
    } else {
      // 基于「LogStartOffset 维度」
      deleteLogStartOffsetBreachedSegments()
    }
  }

  // 基于「时间维度」删除策略
  private def deleteRetentionMsBreachedSegments(): Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds

    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      // 如果当前时间 减去 日志段的最大时间  大于配置的最大时间间隔，则删除
      startMs - segment.largestTimestamp > config.retentionMs
    }

    // 删除日志段，带参数版
    deleteOldSegments(shouldDelete, RetentionMsBreach)
  }

  // 基于「空间维度」删除策略
  private def deleteRetentionSizeBreachedSegments(): Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    // 计算超过最大大小多少字节
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }

    // 删除日志段，带参数版
    deleteOldSegments(shouldDelete, RetentionSizeBreach)
  }

  // 基于「LogStartOffset 维度」删除策略
  private def deleteLogStartOffsetBreachedSegments(): Int = {
    def shouldDelete(segment: LogSegment, nextSegmentOpt: Option[LogSegment]): Boolean = {
      nextSegmentOpt.exists(_.baseOffset <= logStartOffset)
    }

    // 删除日志段，带参数版
    deleteOldSegments(shouldDelete, StartOffsetBreach)
  }

  def isFuture: Boolean = dir.getName.endsWith(Log.FutureDirSuffix)

  /**
   * The size of the log in bytes
   */
  def size: Long = Log.sizeInBytes(logSegments)

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   * The offset of the next message that will be appended to the log
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes.
   * @param appendInfo log append information
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int, appendInfo: LogAppendInfo): LogSegment = {
    // 获取当前活跃的日志段 也就是最后一个 Segment。
    val segment = activeSegment
    // 获取当前系统时间
    val now = time.milliseconds

    // 获取消息中的最大时间戳和最大偏移量
    val maxTimestampInMessages = appendInfo.maxTimestamp
    val maxOffsetInMessages = appendInfo.lastOffset

    // 判断是否需要创建一个新日志段
    if (segment.shouldRoll(RollParams(config, appendInfo, messagesSize, now))) {

      // 打印调试信息
      debug(s"Rolling new log segment (log_size = ${segment.size}/${config.segmentSize}}, " +
        s"offset_index_size = ${segment.offsetIndex.entries}/${segment.offsetIndex.maxEntries}, " +
        s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
        s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")

      /*
        maxOffsetInMessages - Integer.MAX_VALUE is a heuristic value for the first offset in the set of messages.
        Since the offset in messages will not differ by more than Integer.MAX_VALUE, this is guaranteed <= the real
        first offset in the set. Determining the true first offset in the set requires decompression, which the follower
        is trying to avoid during log append. Prior behavior assigned new baseOffset = logEndOffset from old segment.
        This was problematic in the case that two consecutive messages differed in offset by
        Integer.MAX_VALUE.toLong + 2 or more.  In this case, the prior behavior would roll a new log segment whose
        base offset was too low to contain the next message.  This edge case is possible when a replica is recovering a
        highly compacted topic from scratch.
        Note that this is only required for pre-V2 message formats because these do not store the first message offset
        in the header.
      */
      appendInfo.firstOffset match {
        // 如果消息中有第一个偏移量，就使用它来创建新日志段
        case Some(firstOffset) => roll(Some(firstOffset))
        // 如果消息中不存在第一个偏移量，则使用一个启发式值来创建新日志段
        case None => roll(Some(maxOffsetInMessages - Integer.MAX_VALUE))
      }
    } else {
      // 如果不需要创建新日志段，则返回当前活跃的日志段
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   *
   * @return The newly rolled segment
   */
  def roll(expectedNextOffset: Option[Long] = None): LogSegment = {
    maybeHandleIOException(s"Error while rolling log segment for $topicPartition in dir ${dir.getParent}") {
      val start = time.hiResClockMs()
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        // 获取 LEO 值。
        val newOffset = math.max(expectedNextOffset.getOrElse(0L), logEndOffset)
        // 基于 LEO 值生成日志分段文件。
        val logFile = Log.logFile(dir, newOffset)

        // 判断日志段是否存在。
        if (segments.containsKey(newOffset)) {
          // segment with the same base offset already exists and loaded
          // 判断是否有相同 baseffset 的 segment 已经加载进内存了。
          if (activeSegment.baseOffset == newOffset && activeSegment.size == 0) {
            // We have seen this happen (see KAFKA-6388) after shouldRoll() returns true for an
            // active segment of size zero because of one of the indexes is "full" (due to _maxEntries == 0).
            warn(s"Trying to roll a new log segment with start offset $newOffset " +
                 s"=max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already " +
                 s"exists and is active with size 0. Size of time index: ${activeSegment.timeIndex.entries}," +
                 s" size of offset index: ${activeSegment.offsetIndex.entries}.")
            // 存在就删除
            removeAndDeleteSegments(Seq(activeSegment), asyncDelete = true, LogRoll)
          } else {
            throw new KafkaException(s"Trying to roll a new log segment for topic partition $topicPartition with start offset $newOffset" +
                                     s" =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) while it already exists. Existing " +
                                     s"segment is ${segments.get(newOffset)}.")
          }
        } else if (!segments.isEmpty && newOffset < activeSegment.baseOffset) {
          throw new KafkaException(
            s"Trying to roll a new log segment for topic partition $topicPartition with " +
            s"start offset $newOffset =max(provided offset = $expectedNextOffset, LEO = $logEndOffset) lower than start offset of the active segment $activeSegment")
        } else {
          // 基于 LEO 值生成一个日志索引文件
          val offsetIdxFile = offsetIndexFile(dir, newOffset)
          // 基于 LEO 值生成一个时间索引文件
          val timeIdxFile = timeIndexFile(dir, newOffset)
          // 基于 LEO 值生成一个事务索引文件
          val txnIdxFile = transactionIndexFile(dir, newOffset)

          for (file <- List(logFile, offsetIdxFile, timeIdxFile, txnIdxFile) if file.exists) {
            warn(s"Newly rolled segment file ${file.getAbsolutePath} already exists; deleting it first")
            // 删除文件
            Files.delete(file.toPath)
          }

          Option(segments.lastEntry).foreach(_.getValue.onBecomeInactiveSegment())
        }

        // take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
        // offset align with the new segment offset since this ensures we can recover the segment by beginning
        // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
        // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
        // we manually override the state offset here prior to taking the snapshot.
        producerStateManager.updateMapEndOffset(newOffset)
        producerStateManager.takeSnapshot()
        // 新建一个 LogSegment 对象并加入集合中
        val segment = LogSegment.open(dir,
          baseOffset = newOffset,
          config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate)
        // 加入 segment 集合中。
        addSegment(segment)

        // We need to update the segment base offset and append position data of the metadata when log rolls.
        // The next offset should not change.
        // 更新 LEO 值。
        updateLogEndOffset(nextOffsetMetadata.messageOffset)

        // schedule an asynchronous flush of the old segment
        // 异步执行 flush 操作，从恢复点到最新的 offset 都需要 flush。
        scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

        info(s"Rolled new log segment at offset $newOffset in ${time.hiResClockMs() - start} ms.")

        // 返回 segment
        segment
      }
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages: Long = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   *
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long): Unit = {
    maybeHandleIOException(s"Error while flushing log for $topicPartition in dir ${dir.getParent} with offset $offset") {
      if (offset <= this.recoveryPoint)
        return
      debug(s"Flushing log up to offset $offset, last flushed: $lastFlushTime,  current time: ${time.milliseconds()}, " +
        s"unflushed: $unflushedMessages")
      // 遍历所有 logSegment 对象，在（recoveryPoint，offset）区间内的 offset 都要刷盘。
      for (segment <- logSegments(this.recoveryPoint, offset))
      // 调用了 LogSegment 的 flush 方法。
        segment.flush()

      // 修改 recoveryPoint 和 lastFlushedTime 的值
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        if (offset > this.recoveryPoint) {
          this.recoveryPoint = offset // 更新 recoveryPoint.
          lastFlushedTime.set(time.milliseconds) // 修改 lastFlushedTime.
        }
      }
    }
  }

  /**
   * Cleanup old producer snapshots after the recovery point is checkpointed. It is useful to retain
   * the snapshots from the recent segments in case we need to truncate and rebuild the producer state.
   * Otherwise, we would always need to rebuild from the earliest segment.
   *
   * More specifically:
   *
   * 1. We always retain the producer snapshot from the last two segments. This solves the common case
   * of truncating to an offset within the active segment, and the rarer case of truncating to the previous segment.
   *
   * 2. We only delete snapshots for offsets less than the recovery point. The recovery point is checkpointed
   * periodically and it can be behind after a hard shutdown. Since recovery starts from the recovery point, the logic
   * of rebuilding the producer snapshots in one pass and without loading older segments is simpler if we always
   * have a producer snapshot for all segments being recovered.
   *
   * Return the minimum snapshots offset that was retained.
   */
  // 用来删除某个日志文件的恢复点检查点后的快照
  def deleteSnapshotsAfterRecoveryPointCheckpoint(): Long = {
    // 首先计算一个最小的偏移量，该偏移量以及其之前的快照文件将被保留
    val minOffsetToRetain = minSnapshotsOffsetToRetain
    // 删除该文件中偏移量小于 minOffsetToRetain 的快照文件
    producerStateManager.deleteSnapshotsBefore(minOffsetToRetain)
    // 返回 minOffsetToRetain
    minOffsetToRetain
  }

  // Visible for testing, see `deleteSnapshotsAfterRecoveryPointCheckpoint()` for details
  // 获取保留快照的最小偏移量，即偏移量之前的快照文件将被保留
  private[log] def minSnapshotsOffsetToRetain: Long = {
    // 获取日志文件的锁
    lock synchronized {
      // 计算要保留的最小偏移量，该偏移量以及其之前的快照文件将被保留
      val twoSegmentsMinOffset = lowerSegment(activeSegment.baseOffset).getOrElse(activeSegment).baseOffset
      // Prefer segment base offset
      // 计算恢复点偏移量
      val recoveryPointOffset = lowerSegment(recoveryPoint).map(_.baseOffset).getOrElse(recoveryPoint)
      // 根据最小偏移量和恢复点计算一个偏移量。
      math.min(recoveryPointOffset, twoSegmentsMinOffset)
    }
  }

  // 找到最近的存储上给定偏移量的日志段
  // 接收一个偏移量并返回其之前的日志段。 它基于 segments，该变量是日志文件中所有日志段的列表，每个日志段具有开始和结束偏移量。
  // 因为 segments 是按基础偏移排序的，所以可以使用 lowerEntry 方法来查找最近的日志段。
  private def lowerSegment(offset: Long): Option[LogSegment] =
  //  lowerEntry 方法在 segments 列表中查找最后一个元素，其基础偏移量小于指定的偏移量。
    Option(segments.lowerEntry(offset)).map(_.getValue)

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete(): Unit = {
    maybeHandleIOException(s"Error while deleting log for $topicPartition in dir ${dir.getParent}") {
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        producerExpireCheck.cancel(true)
        removeAndDeleteSegments(logSegments, asyncDelete = false, LogDeletion)
        leaderEpochCache.foreach(_.clear())
        Utils.delete(dir)
        // File handlers will be closed if this log is deleted
        isMemoryMappedBufferClosed = true
      }
    }
  }

  // visible for testing
  private[log] def takeProducerSnapshot(): Unit = lock synchronized {
    checkIfMemoryMappedBufferClosed()
    producerStateManager.takeSnapshot()
  }

  // visible for testing
  private[log] def latestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.latestSnapshotOffset
  }

  // visible for testing
  private[log] def oldestProducerSnapshotOffset: Option[Long] = lock synchronized {
    producerStateManager.oldestSnapshotOffset
  }

  // visible for testing
  private[log] def latestProducerStateEndOffset: Long = lock synchronized {
    producerStateManager.mapEndOffset
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   * @return True iff targetOffset < logEndOffset
   */
  private[kafka] def truncateTo(targetOffset: Long): Boolean = {
    maybeHandleIOException(s"Error while truncating log to offset $targetOffset for $topicPartition in dir ${dir.getParent}") {
      if (targetOffset < 0)
        throw new IllegalArgumentException(s"Cannot truncate partition $topicPartition to a negative offset (%d).".format(targetOffset))
      if (targetOffset >= logEndOffset) {
        info(s"Truncating to $targetOffset has no effect as the largest offset in the log is ${logEndOffset - 1}")

        // Always truncate epoch cache since we may have a conflicting epoch entry at the
        // end of the log from the leader. This could happen if this broker was a leader
        // and inserted the first start offset entry, but then failed to append any entries
        // before another leader was elected.
        lock synchronized {
          leaderEpochCache.foreach(_.truncateFromEnd(logEndOffset))
        }

        false
      } else {
        info(s"Truncating to offset $targetOffset")
        lock synchronized {
          checkIfMemoryMappedBufferClosed()
          if (segments.firstEntry.getValue.baseOffset > targetOffset) {
            truncateFullyAndStartAt(targetOffset)
          } else {
            val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
            removeAndDeleteSegments(deletable, asyncDelete = true, LogTruncation)
            activeSegment.truncateTo(targetOffset)
            updateLogEndOffset(targetOffset)
            updateLogStartOffset(math.min(targetOffset, this.logStartOffset))
            leaderEpochCache.foreach(_.truncateFromEnd(targetOffset))
            loadProducerState(targetOffset, reloadFromCleanShutdown = false)
          }
          true
        }
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long): Unit = {
    maybeHandleIOException(s"Error while truncating the entire log for $topicPartition in dir ${dir.getParent}") {
      debug(s"Truncate and start at offset $newOffset")
      lock synchronized {
        checkIfMemoryMappedBufferClosed()
        removeAndDeleteSegments(logSegments, asyncDelete = true, LogTruncation)
        addSegment(LogSegment.open(dir,
          baseOffset = newOffset,
          config = config,
          time = time,
          fileAlreadyExists = false,
          initFileSize = initFileSize,
          preallocate = config.preallocate))
        updateLogEndOffset(newOffset)
        leaderEpochCache.foreach(_.clearAndFlush())

        producerStateManager.truncate()
        producerStateManager.updateMapEndOffset(newOffset)
        maybeIncrementFirstUnstableOffset()
        updateLogStartOffset(newOffset)
      }
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime: Long = lastFlushedTime.get

  /**
   * The active segment that is currently taking appends
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = segments.values.asScala

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset).
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    if (from == to) {
      // Handle non-segment-aligned empty sets
      List.empty[LogSegment]
    } else if (to < from) {
      throw new IllegalArgumentException(s"Invalid log segment range: requested segments in $topicPartition " +
        s"from offset $from which is greater than limit offset $to")
    } else {
      lock synchronized {
        val view = Option(segments.floorKey(from)).map { floor =>
          segments.subMap(floor, to)
        }.getOrElse(segments.headMap(to))
        view.values.asScala
      }
    }
  }

  def nonActiveLogSegmentsFrom(from: Long): Iterable[LogSegment] = {
    lock synchronized {
      if (from > activeSegment.baseOffset)
        Seq.empty
      else
        logSegments(from, activeSegment.baseOffset)
    }
  }

  /**
   * Get the largest log segment with a base offset less than or equal to the given offset, if one exists.
   * @return the optional log segment
   */
  private def floorLogSegment(offset: Long): Option[LogSegment] = {
    Option(segments.floorEntry(offset)).map(_.getValue)
  }

  override def toString: String = {
    val logString = new StringBuilder
    logString.append(s"Log(dir=$dir")
    logString.append(s", topic=${topicPartition.topic}")
    logString.append(s", partition=${topicPartition.partition}")
    logString.append(s", highWatermark=$highWatermark")
    logString.append(s", lastStableOffset=$lastStableOffset")
    logString.append(s", logStartOffset=$logStartOffset")
    logString.append(s", logEndOffset=$logEndOffset")
    logString.append(")")
    logString.toString
  }

  /**
   * This method deletes the given log segments by doing the following for each of them:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It can either schedule an asynchronous delete operation to occur in the future or perform the deletion synchronously
   * </ol>
   * Asynchronous deletion allows reads to happen concurrently without synchronization and without the possibility of
   * physically deleting a file while it is being read.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the immediate caller will catch and handle IOException
   *
   * @param segments The log segments to schedule for deletion
   * @param asyncDelete Whether the segment files should be deleted asynchronously
   */
  private def removeAndDeleteSegments(segments: Iterable[LogSegment],
                                      asyncDelete: Boolean,
                                      reason: SegmentDeletionReason): Unit = {
    if (segments.nonEmpty) {
      lock synchronized {
        // As most callers hold an iterator into the `segments` collection and `removeAndDeleteSegment` mutates it by
        // removing the deleted segment, we should force materialization of the iterator here, so that results of the
        // iteration remain valid and deterministic.
        val toDelete = segments.toList
        reason.logReason(this, toDelete)
        toDelete.foreach { segment =>
          this.segments.remove(segment.baseOffset)
        }
        deleteSegmentFiles(toDelete, asyncDelete)
      }
    }
  }

  /**
   * Perform physical deletion for the given file. Allows the file to be deleted asynchronously or synchronously.
   *
   * This method assumes that the file exists and the method is not thread-safe.
   *
   * This method does not need to convert IOException (thrown from changeFileSuffixes) to KafkaStorageException because
   * it is either called before all logs are loaded or the caller will catch and handle IOException
   *
   * @throws IOException if the file can't be renamed and still exists
   */
  private def deleteSegmentFiles(segments: Iterable[LogSegment], asyncDelete: Boolean): Unit = {
    segments.foreach(_.changeFileSuffixes("", Log.DeletedFileSuffix))

    def deleteSegments(): Unit = {
      info(s"Deleting segment files ${segments.mkString(",")}")
      maybeHandleIOException(s"Error while deleting segments for $topicPartition in dir ${dir.getParent}") {
        segments.foreach(_.deleteIfExists())
      }
    }

    if (asyncDelete)
      scheduler.schedule("delete-file", () => deleteSegments(), delay = config.fileDeleteDelayMs)
    else
      deleteSegments()
  }

  /**
   * Swap one or more new segment in place and delete one or more existing segments in a crash-safe manner. The old
   * segments will be asynchronously deleted.
   *
   * This method does not need to convert IOException to KafkaStorageException because it is either called before all logs are loaded
   * or the caller will catch and handle IOException
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates one or more new segments with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned files are deleted on recovery in loadSegments().
   *   <li> New segments are renamed .swap. If the broker crashes before all segments were renamed to .swap, the
   *        clean-and-swap operation is aborted - .cleaned as well as .swap files are deleted on recovery in
   *        loadSegments(). We detect this situation by maintaining a specific order in which files are renamed from
   *        .cleaned to .swap. Basically, files are renamed in descending order of offsets. On recovery, all .swap files
   *        whose offset is greater than the minimum-offset .clean file are deleted.
   *   <li> If the broker crashes after all new segments were renamed to .swap, the operation is completed, the swap
   *        operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment(s) are renamed to replace the existing segments, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegments The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */

  /*
   * 该方法的作用是用新的日志段文件替换旧的日志段文件。该方法将旧的日志段文件删除，并将新的日志段文
   * 件添加到 Log 实例的 segments 列表中。当然，该方法还保证了异步删除旧的日志段文件，同时实现了 crash safe。
   */
  // newSegments 表示新的日志段文件序列
  // oldSegments 表示需要替换的旧的日志段文件序列
  // isRecoveredSwapFile 表示该操作是否针对恢复的 swap 文件
  private[log] def replaceSegments(newSegments: Seq[LogSegment], oldSegments: Seq[LogSegment], isRecoveredSwapFile: Boolean = false): Unit = {
    lock synchronized {
      // 将新的日志段文件按照 baseOffset 排序，存储到 sortedNewSegments 变量中。
      val sortedNewSegments = newSegments.sortBy(_.baseOffset)
      // Some old segments may have been removed from index and scheduled for async deletion after the caller reads segments
      // but before this method is executed. We want to filter out those segments to avoid calling asyncDeleteSegment()
      // multiple times for the same segment.
      // 将需要替换的旧的日志段文件按照 baseOffset 排序，并过滤掉已被 asyncDelete 标记的文件。
      val sortedOldSegments = oldSegments.filter(seg => segments.containsKey(seg.baseOffset)).sortBy(_.baseOffset)

      checkIfMemoryMappedBufferClosed()
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
      // 将所有新的日志段文件的后缀名从 Cleaned 修改为 Swap。
        sortedNewSegments.reverse.foreach(_.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix))
      // 将所有新的日志段文件添加到 Log 实例的 segments 列表中。
      // 这里的 reverse 意义是为了确保 segments 中的日志段能否覆盖包含的 offset 值，因为可能其他线程也要访问 segments
      sortedNewSegments.reverse.foreach(addSegment(_))

      // delete the old files
      // 遍历需要替换的旧的日志段文件。
      for (seg <- sortedOldSegments) {
        // remove the index entry
        // 如果该日志段文件不是最新的日志段文件，则从 segments 列表中移除该日志段文件。
        if (seg.baseOffset != sortedNewSegments.head.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment files
        // 删除该日志段文件，如果 asyncDelete 为 true，则异步删除该文件。
        deleteSegmentFiles(List(seg), asyncDelete = true)
      }
      // okay we are safe now, remove the swap suffix
      // 将所有新的日志段文件的后缀名从 Swap 修改为空。这里是因为当做完了 swap 后就要移除 swap 后缀
      sortedNewSegments.foreach(_.changeFileSuffixes(Log.SwapFileSuffix, ""))
    }
  }

  /**
    * This function does not acquire Log.lock. The caller has to make sure log segments don't get deleted during
    * this call, and also protects against calling this function on the same segment in parallel.
    *
    * Currently, it is used by LogCleaner threads on log compact non-active segments only with LogCleanerManager's lock
    * to ensure no other logcleaner threads and retention thread can work on the same segment.
    */
  private[log] def getFirstBatchTimestampForSegments(segments: Iterable[LogSegment]): Iterable[Long] = {
    segments.map {
      segment =>
        segment.getFirstBatchTimestamp()
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric(LogMetricNames.NumLogSegments, tags)
    removeMetric(LogMetricNames.LogStartOffset, tags)
    removeMetric(LogMetricNames.LogEndOffset, tags)
    removeMetric(LogMetricNames.Size, tags)
  }

  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   * @param segment The segment to add
   */
  @threadsafe
  def addSegment(segment: LogSegment): LogSegment = this.segments.put(segment.baseOffset, segment)

  private def maybeHandleIOException[T](msg: => String)(fun: => T): T = {
    try {
      fun
    } catch {
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir.getParent, msg, e)
        throw new KafkaStorageException(msg, e)
    }
  }

  private[log] def retryOnOffsetOverflow[T](fn: => T): T = {
    while (true) {
      try {
        return fn
      } catch {
        case e: LogSegmentOffsetOverflowException =>
          info(s"Caught segment overflow error: ${e.getMessage}. Split segment and retry.")
          splitOverflowedSegment(e.segment)
      }
    }
    throw new IllegalStateException()
  }

  /**
   * Split a segment into one or more segments such that there is no offset overflow in any of them. The
   * resulting segments will contain the exact same messages that are present in the input segment. On successful
   * completion of this method, the input segment will be deleted and will be replaced by the resulting new segments.
   * See replaceSegments for recovery logic, in case the broker dies in the middle of this operation.
   * <p>Note that this method assumes we have already determined that the segment passed in contains records that cause
   * offset overflow.</p>
   * <p>The split logic overloads the use of .clean files that LogCleaner typically uses to make the process of replacing
   * the input segment with multiple new segments atomic and recoverable in the event of a crash. See replaceSegments
   * and completeSwapOperations for the implementation to make this operation recoverable on crashes.</p>
   * @param segment Segment to split
   * @return List of new segments that replace the input segment
   */
  private[log] def splitOverflowedSegment(segment: LogSegment): List[LogSegment] = {
    require(isLogFile(segment.log.file), s"Cannot split file ${segment.log.file.getAbsoluteFile}")
    require(segment.hasOverflow, "Split operation is only permitted for segments with overflow")

    info(s"Splitting overflowed segment $segment")

    val newSegments = ListBuffer[LogSegment]()
    try {
      var position = 0
      val sourceRecords = segment.log

      while (position < sourceRecords.sizeInBytes) {
        val firstBatch = sourceRecords.batchesFrom(position).asScala.head
        val newSegment = LogCleaner.createNewCleanedSegment(this, firstBatch.baseOffset)
        newSegments += newSegment

        val bytesAppended = newSegment.appendFromFile(sourceRecords, position)
        if (bytesAppended == 0)
          throw new IllegalStateException(s"Failed to append records from position $position in $segment")

        position += bytesAppended
      }

      // prepare new segments
      var totalSizeOfNewSegments = 0
      newSegments.foreach { splitSegment =>
        splitSegment.onBecomeInactiveSegment()
        splitSegment.flush()
        splitSegment.lastModified = segment.lastModified
        totalSizeOfNewSegments += splitSegment.log.sizeInBytes
      }
      // size of all the new segments combined must equal size of the original segment
      if (totalSizeOfNewSegments != segment.log.sizeInBytes)
        throw new IllegalStateException("Inconsistent segment sizes after split" +
          s" before: ${segment.log.sizeInBytes} after: $totalSizeOfNewSegments")

      // replace old segment with new ones
      info(s"Replacing overflowed segment $segment with split segments $newSegments")
      replaceSegments(newSegments.toList, List(segment))
      newSegments.toList
    } catch {
      case e: Exception =>
        newSegments.foreach { splitSegment =>
          splitSegment.close()
          splitSegment.deleteIfExists()
        }
        throw e
    }
  }
}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  // Log 的目录，用于存储日志文件。
  val LogFileSuffix = ".log"

  /** an index file */
  //  Log 的目录，用于存储索引文件。
  val IndexFileSuffix = ".index"

  /** a time index file */
  // Log 的目录，用于存储时间索引文件。
  val TimeIndexFileSuffix = ".timeindex"

  // Kafka 为幂等型或事务型 Producer 所做的快照文件
  val ProducerSnapshotFileSuffix = ".snapshot"

  /** an (aborted) txn index */
  // Kafka 为事务消息所做的已终止事务索引文件。
  val TxnIndexFileSuffix = ".txnindex"

  /** a file that is scheduled to be deleted */
  // 删除日志段操作创建的文件。目前删除日志段文件是异步任务操作，Broker 端把日志段文件从 .log 后缀修改为 .deleted 后缀。
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  //  Compaction 操作的产物
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  // Compaction 操作的产物
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8 and higher.
   * This is used to avoid unnecessary recovery after a clean shutdown. In theory this could be
   * avoided by passing in the recovery point, however finding the correct position to do this
   * requires accessing the offset index which may not be safe in an unclean shutdown.
   * For more information see the discussion in PR#2104
   */
  //  Clean shutdown文件，存在表示 kafka 正在做清理性的停机工作。
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** a directory that is scheduled to be deleted */
  // 主要用在文件夹中的。当你删除一个主题的时候，主题对应分区的文件夹会被加上该后缀。
  val DeleteDirSuffix = "-delete"

  /** a directory that is used for future partition */
  // 主要用在变更主题分区文件夹地址的。
  val FutureDirSuffix = "-future"

  private[log] val DeleteDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$DeleteDirSuffix")
  private[log] val FutureDirPattern = Pattern.compile(s"^(\\S+)-(\\S+)\\.(\\S+)$FutureDirSuffix")

  val UnknownOffset = -1L

  // 用于创建一个 Log 实例
  def apply(dir: File, // Log 的目录，用于存储日志文件。
            config: LogConfig, // Log 的配置信息。
            logStartOffset: Long, // 该 Log 起始的 offset 值。
            recoveryPoint: Long, // 该 Log 的恢复点，表示之前的数据可以丢弃。
            scheduler: Scheduler, // 用于调度一些异步任务的 scheduler。
            brokerTopicStats: BrokerTopicStats, // 记录 kafka Broker 级别的一些统计信息。
            time: Time = Time.SYSTEM, // kafka 时间类库，用于获取当前时间等操作。
            maxProducerIdExpirationMs: Int, // 设置 Producer ID 的最大过期时间。
            producerIdExpirationCheckIntervalMs: Int, // 设置检查 Producer ID 过期的时间间隔。
            logDirFailureChannel: LogDirFailureChannel): Log = { // Kafka 监测 Log 目录故障的通道。
    // 解析出 Log 存储的 topic 和 partition。
    val topicPartition = Log.parseTopicPartitionName(dir)
    // 创建 ProducerStateManager 实例，用于维护 producerId 和 producerEpoch 在消息可靠性保证方面的作用。
    val producerStateManager = new ProducerStateManager(topicPartition, dir, maxProducerIdExpirationMs)
    // 使用给定的参数创建一个 Log 对象，该对象代表了一个 Log 目录。
    new Log(dir, config, logStartOffset, recoveryPoint, scheduler, brokerTopicStats, time, maxProducerIdExpirationMs,
      producerIdExpirationCheckIntervalMs, topicPartition, producerStateManager, logDirFailureChannel)
  }

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   *
   * @param offset The offset to use in the file name
   * @return The filename
   */

  /*
   * 通过给定的位移值计算出对应的日志段文件名。这仅仅是用 0 填充偏移量数字，以使得 ls 命令按数值顺序排列。
   *
   * @param offset 文件名中要包含的偏移量
   * @return 文件名
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    // Kafka 日志文件固定是 20 位的长度
    nf.setMinimumIntegerDigits(20)
    // 设置用前面补 0 的方式把给定位移值扩充成一个固定 20 位长度的字符串。
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name (e.g. "", ".deleted", ".cleaned", ".swap", etc.)
   */

  /*
   * 用给定的偏移量、指定的后缀构建在指定目录中的日志文件名
   *
   * @param dir    日志文件所在的目录
   * @param offset 文件名中的起始偏移量
   * @param suffix 要附加的后缀名称（例如“”，“.deleted”，“.cleaned”，“.swap”等）
   */
  def logFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix + suffix)

  /**
   * Return a directory name to rename the log directory to for async deletion.
   * The name will be in the following format: "topic-partitionId.uniqueId-delete".
   * If the topic name is too long, it will be truncated to prevent the total name
   * from exceeding 255 characters.
   */

  /*
   * 返回一个目录名称，用于重命名日志目录以进行异步删除。
   * 名称将采用以下格式："topic-partitionId.uniqueId-delete"。
   * 如果主题名称太长，则将其截断以防止总名称超过 255 个字符。
   */
  def logDeleteDirName(topicPartition: TopicPartition): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    val suffix = s"-${topicPartition.partition()}.${uniqueId}${DeleteDirSuffix}"
    val prefixLength = Math.min(topicPartition.topic().size, 255 - suffix.size)
    s"${topicPartition.topic().substring(0, prefixLength)}${suffix}"
  }

  /**
   * Return a future directory name for the given topic partition. The name will be in the following
   * format: topic-partition.uniqueId-future where topic, partition and uniqueId are variables.
   */

  /*
   * 返回为给定主题分区的未来目录名称。
   * 名称将采用以下格式：topic-partition.uniqueId-future，其中 topic、partition 和 uniqueId 是变量。
   */
  def logFutureDirName(topicPartition: TopicPartition): String = {
    logDirNameWithSuffix(topicPartition, FutureDirSuffix)
  }

  private def logDirNameWithSuffix(topicPartition: TopicPartition, suffix: String): String = {
    val uniqueId = java.util.UUID.randomUUID.toString.replaceAll("-", "")
    s"${logDirName(topicPartition)}.$uniqueId$suffix"
  }

  /**
   * Return a directory name for the given topic partition. The name will be in the following
   * format: topic-partition where topic, partition are variables.
   */

  /*
   * 返回给定主题分区的目录名称。名称将采用以下格式："topic-partition"，其中 topic 和 partition 是变量。
   */
  def logDirName(topicPartition: TopicPartition): String = {
    s"${topicPartition.topic}-${topicPartition.partition}"
  }

  /**
   * Construct an index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */

  /*
   * 在给定的目录中使用给定的起始偏移量和给定的后缀名称构建索引文件名
   *
   * @param dir    日志所在的目录
   * @param offset 文件名中的起始偏移量
   * @param suffix 要附加的后缀名称（“”、“.deleted”、“.cleaned”、“.swap”等）
   */
  def offsetIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix + suffix)

  /**
   * Construct a time index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */

  /*
   * 在给定的目录中使用给定的起始偏移量和给定的后缀名称构建时间索引文件名
   *
   * @param dir    日志所在的目录
   * @param offset 文件名中的起始偏移量
   * @param suffix 要附加的后缀名称（“”、“.deleted”、“.cleaned”、“.swap”等）
   */
  def timeIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix + suffix)

  /*
   * 删除文件（如果存在）并附加后缀。suffix 默认为 ""，也可指定其他后缀。
   *
   * @param file   要删除的文件
   * @param suffix 要添加的后缀（默认为 ""）
   */
  def deleteFileIfExists(file: File, suffix: String = ""): Unit =
    Files.deleteIfExists(new File(file.getPath + suffix).toPath)

  /**
   * Construct a producer id snapshot file using the given offset.
   *
   * @param dir The directory in which the log will reside
   * @param offset The last offset (exclusive) included in the snapshot
   */

  /*
   * 使用给定的偏移量构建生产者 ID 快照文件名称。
   *
   * @param dir    日志所在的目录
   * @param offset 快照中包含的最后一个偏移量（独占）。
   */
  def producerSnapshotFile(dir: File, offset: Long): File =
    new File(dir, filenamePrefixFromOffset(offset) + ProducerSnapshotFileSuffix)

  /**
   * Construct a transaction index file name in the given dir using the given base offset and the given suffix
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   * @param suffix The suffix to be appended to the file name ("", ".deleted", ".cleaned", ".swap", etc.)
   */

  /*
   * 在给定的目录中使用给定的起始偏移量和给定的后缀名称构建事务索引文件名
   *
   * @param dir    日志所在的目录
   * @param offset 文件名中的起始偏移量
   * @param suffix 要附加的后缀名称（“”、“.deleted”、“.cleaned”、“.swap”等）
   */
  def transactionIndexFile(dir: File, offset: Long, suffix: String = ""): File =
    new File(dir, filenamePrefixFromOffset(offset) + TxnIndexFileSuffix + suffix)

  // 从文件名中获取位移信息
  // filename 表示待解析的日志文件名
  def offsetFromFileName(filename: String): Long = {
    // 该方法首先使用 indexOf 方法查找文件名中第一个点号的位置，然后使用 substring 方法获取点号之前的字符串部分，
    // 即 offset 的值，最后使用 toLong 方法将其转换为 Long 类型并返回。例如，对于文件名 "00000000000000000000.log"，
    // 该方法将返回 Long 类型的 0。
    filename.substring(0, filename.indexOf('.')).toLong
  }

  // 从文件中获取位移信息
  // file 表示待解析的日志文件
  def offsetFromFile(file: File): Long = {
    // 该方法首先调用 file.getName 方法获取文件名，然后调用 offsetFromFileName 方法解析文件名，并返回对应的 offset 值。
    offsetFromFileName(file.getName)
  }

  /**
   * Calculate a log's size (in bytes) based on its log segments
   *
   * @param segments The log segments to calculate the size of
   * @return Sum of the log segments' sizes (in bytes)
   */

  /*
   * 计算一个日志的大小（以字节为单位），基于其日志段。
   *
   * @param segments 要计算大小的日志段。
   * @return 日志段大小（以字节为单位）的总和。
   */
  def sizeInBytes(segments: Iterable[LogSegment]): Long =
    segments.map(_.size.toLong).sum

  /**
   * Parse the topic and partition out of the directory name of a log
   */

  /*
   * 在日志目录名称中解析出主题和分区。
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {
    if (dir == null)
      throw new KafkaException("dir should not be null")

    // 异常处理
    def exception(dir: File): KafkaException = {
      new KafkaException(s"Found directory ${dir.getCanonicalPath}, '${dir.getName}' is not in the form of " +
        "topic-partition or topic-partition.uniqueId-delete (if marked for deletion).\n" +
        "Kafka's log directories (and children) should only contain Kafka topic data.")
    }

    // 目录名
    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)
    // 匹配判断,不符合抛异常
    if (dirName.endsWith(DeleteDirSuffix) && !DeleteDirPattern.matcher(dirName).matches ||
        dirName.endsWith(FutureDirSuffix) && !FutureDirPattern.matcher(dirName).matches)
      throw exception(dir)

    // 获取目录名
    val name: String =
      if (dirName.endsWith(DeleteDirSuffix) || dirName.endsWith(FutureDirSuffix)) dirName.substring(0, dirName.lastIndexOf('.'))
      else dirName

    val index = name.lastIndexOf('-')
    val topic = name.substring(0, index)
    val partitionString = name.substring(index + 1)
    if (topic.isEmpty || partitionString.isEmpty)
      throw exception(dir)

    val partition =
      try partitionString.toInt
      catch { case _: NumberFormatException => throw exception(dir) }

    new TopicPartition(topic, partition)
  }

  private def isIndexFile(file: File): Boolean = {
    val filename = file.getName
    filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix) || filename.endsWith(TxnIndexFileSuffix)
  }

  private def isLogFile(file: File): Boolean =
    file.getPath.endsWith(LogFileSuffix)

}

object LogMetricNames {
  val NumLogSegments: String = "NumLogSegments"
  val LogStartOffset: String = "LogStartOffset"
  val LogEndOffset: String = "LogEndOffset"
  val Size: String = "Size"

  def allMetricNames: List[String] = {
    List(NumLogSegments, LogStartOffset, LogEndOffset, Size)
  }
}

sealed trait SegmentDeletionReason {
  def logReason(log: Log, toDelete: List[LogSegment]): Unit
}

case object RetentionMsBreach extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    val retentionMs = log.config.retentionMs
    toDelete.foreach { segment =>
      segment.largestRecordTimestamp match {
        case Some(_) =>
          log.info(s"Deleting segment $segment due to retention time ${retentionMs}ms breach based on the largest " +
            s"record timestamp in the segment")
        case None =>
          log.info(s"Deleting segment $segment due to retention time ${retentionMs}ms breach based on the " +
            s"last modified time of the segment")
      }
    }
  }
}

case object RetentionSizeBreach extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    var size = log.size
    toDelete.foreach { segment =>
      size -= segment.size
      log.info(s"Deleting segment $segment due to retention size ${log.config.retentionSize} breach. Log size " +
        s"after deletion will be $size.")
    }
  }
}

case object StartOffsetBreach extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments due to log start offset ${log.logStartOffset} breach: ${toDelete.mkString(",")}")
  }
}

case object LogRecovery extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as part of log recovery: ${toDelete.mkString(",")}")
  }
}

case object LogTruncation extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as part of log truncation: ${toDelete.mkString(",")}")
  }
}

case object LogRoll extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as part of log roll: ${toDelete.mkString(",")}")
  }
}

case object LogDeletion extends SegmentDeletionReason {
  override def logReason(log: Log, toDelete: List[LogSegment]): Unit = {
    log.info(s"Deleting segments as the log has been deleted: ${toDelete.mkString(",")}")
  }
}