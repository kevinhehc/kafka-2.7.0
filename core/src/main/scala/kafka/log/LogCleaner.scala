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
import java.nio._
import java.util.Date
import java.util.concurrent.TimeUnit

import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerReconfigurable, KafkaConfig, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.{CorruptRecordException, KafkaStorageException}
import org.apache.kafka.common.record.MemoryRecords.RecordFilter
import org.apache.kafka.common.record.MemoryRecords.RecordFilter.BatchRetention
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Iterable, Seq, Set, mutable}
import scala.util.control.ControlThrowable

/**
 * The cleaner is responsible for removing obsolete records from logs which have the "compact" retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 *
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The dirty section is further divided into the "cleanable" section followed by an "uncleanable" section.
 * The uncleanable section is excluded from cleaning. The active log segment is always uncleanable. If there is a
 * compaction lag time set, segments whose largest message timestamp is within the compaction lag time of the cleaning operation are also uncleanable.
 *
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "compact" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 *
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping.
 *
 * Once the key=>last_offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 *
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 *
 * Cleaned segments are swapped into the log as they become available.
 *
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 *
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 *
 * Note that cleaning is more complicated with the idempotent/transactional producer capabilities. The following
 * are the key points:
 *
 * 1. In order to maintain sequence number continuity for active producers, we always retain the last batch
 *    from each producerId, even if all the records from the batch have been removed. The batch will be removed
 *    once the producer either writes a new batch or is expired due to inactivity.
 * 2. We do not clean beyond the last stable offset. This ensures that all records observed by the cleaner have
 *    been decided (i.e. committed or aborted). In particular, this allows us to use the transaction index to
 *    collect the aborted transactions ahead of time.
 * 3. Records from aborted transactions are removed by the cleaner immediately without regard to record keys.
 * 4. Transaction markers are retained until all record batches from the same transaction have been removed and
 *    a sufficient amount of time has passed to reasonably ensure that an active consumer wouldn't consume any
 *    data from the transaction prior to reaching the offset of the marker. This follows the same logic used for
 *    tombstone deletion.
 *
 * @param initialConfig Initial configuration parameters for the cleaner. Actual config may be dynamically updated.
 * @param logDirs The directories where offset checkpoints reside
 * @param logs The pool of logs
 * @param time A way to control the passage of time
 */
class LogCleaner(initialConfig: CleanerConfig,
                 val logDirs: Seq[File],
                 val logs: Pool[TopicPartition, Log],
                 val logDirFailureChannel: LogDirFailureChannel,
                 time: Time = Time.SYSTEM) extends Logging with KafkaMetricsGroup with BrokerReconfigurable
{

  /* Log cleaner configuration which may be dynamically updated */
  @volatile private var config = initialConfig

  /* for managing the state of partitions being cleaned. package-private to allow access in tests */
  private[log] val cleanerManager = new LogCleanerManager(logDirs, logs, logDirFailureChannel)

  /* a throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate */
  private val throttler = new Throttler(desiredRatePerSec = config.maxIoBytesPerSecond,
                                        checkIntervalMs = 300,
                                        throttleDown = true,
                                        "cleaner-io",
                                        "bytes",
                                        time = time)

  private[log] val cleaners = mutable.ArrayBuffer[CleanerThread]()

  /**
   * scala 2.12 does not support maxOption so we handle the empty manually.
   * @param f to compute the result
   * @return the max value (int value) or 0 if there is no cleaner
   */
  private def maxOverCleanerThreads(f: CleanerThread => Double): Int =
    cleaners.foldLeft(0.0d)((max: Double, thread: CleanerThread) => math.max(max, f(thread))).toInt


  /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
  newGauge("max-buffer-utilization-percent",
    () => maxOverCleanerThreads(_.lastStats.bufferUtilization) * 100)

  /* a metric to track the recopy rate of each thread's last cleaning */
  newGauge("cleaner-recopy-percent", () => {
    val stats = cleaners.map(_.lastStats)
    val recopyRate = stats.iterator.map(_.bytesWritten).sum.toDouble / math.max(stats.iterator.map(_.bytesRead).sum, 1)
    (100 * recopyRate).toInt
  })

  /* a metric to track the maximum cleaning time for the last cleaning from each thread */
  newGauge("max-clean-time-secs",
    () => maxOverCleanerThreads(_.lastStats.elapsedSecs))


  // a metric to track delay between the time when a log is required to be compacted
  // as determined by max compaction lag and the time of last cleaner run.
  newGauge("max-compaction-delay-secs",
    () => maxOverCleanerThreads(_.lastPreCleanStats.maxCompactionDelayMs.toDouble) / 1000)

  newGauge("DeadThreadCount", () => deadThreadCount)

  private[log] def deadThreadCount: Int = cleaners.count(_.isThreadFailed)

  /**
   * Start the background cleaning
   */
  def startup(): Unit = {
    info("Starting the log cleaner")
    (0 until config.numThreads).foreach { i =>
      val cleaner = new CleanerThread(i)
      cleaners += cleaner
      cleaner.start()
    }
  }

  /**
   * Stop the background cleaning
   */
  def shutdown(): Unit = {
    info("Shutting down the log cleaner.")
    cleaners.foreach(_.shutdown())
    cleaners.clear()
  }

  override def reconfigurableConfigs: Set[String] = {
    LogCleaner.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    val newCleanerConfig = LogCleaner.cleanerConfig(newConfig)
    val numThreads = newCleanerConfig.numThreads
    val currentThreads = config.numThreads
    if (numThreads < 1)
      throw new ConfigException(s"Log cleaner threads should be at least 1")
    if (numThreads < currentThreads / 2)
      throw new ConfigException(s"Log cleaner threads cannot be reduced to less than half the current value $currentThreads")
    if (numThreads > currentThreads * 2)
      throw new ConfigException(s"Log cleaner threads cannot be increased to more than double the current value $currentThreads")

  }

  /**
    * Reconfigure log clean config. This simply stops current log cleaners and creates new ones.
    * That ensures that if any of the cleaners had failed, new cleaners are created to match the new config.
    */
  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    config = LogCleaner.cleanerConfig(newConfig)
    shutdown()
    startup()
  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    cleanerManager.abortCleaning(topicPartition)
  }

  /**
   * Update checkpoint file to remove partitions if necessary.
   */
  def updateCheckpoints(dataDir: File, partitionToRemove: Option[TopicPartition] = None): Unit = {
    cleanerManager.updateCheckpoints(dataDir, partitionToRemove = partitionToRemove)
  }

  /**
   * alter the checkpoint directory for the topicPartition, to remove the data in sourceLogDir, and add the data in destLogDir
   */
  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    cleanerManager.alterCheckpointDir(topicPartition, sourceLogDir, destLogDir)
  }

  /**
   * Stop cleaning logs in the provided directory
   *
   * @param dir     the absolute path of the log dir
   */
  def handleLogDirFailure(dir: String): Unit = {
    cleanerManager.handleLogDirFailure(dir)
  }

  /**
   * Truncate cleaner offset checkpoint for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long): Unit = {
    cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset)
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    cleanerManager.abortAndPauseCleaning(topicPartition)
  }

  /**
    *  Resume the cleaning of paused partitions.
    */
  def resumeCleaning(topicPartitions: Iterable[TopicPartition]): Unit = {
    cleanerManager.resumeCleaning(topicPartitions)
  }

  /**
   * For testing, a way to know when work has completed. This method waits until the
   * cleaner has processed up to the given offset on the specified topic/partition
   *
   * @param topicPartition The topic and partition to be cleaned
   * @param offset The first dirty offset that the cleaner doesn't have to clean
   * @param maxWaitMs The maximum time in ms to wait for cleaner
   *
   * @return A boolean indicating whether the work has completed before timeout
   */
  def awaitCleaned(topicPartition: TopicPartition, offset: Long, maxWaitMs: Long = 60000L): Boolean = {
    def isCleaned = cleanerManager.allCleanerCheckpoints.get(topicPartition).fold(false)(_ >= offset)
    var remainingWaitMs = maxWaitMs
    while (!isCleaned && remainingWaitMs > 0) {
      val sleepTime = math.min(100, remainingWaitMs)
      Thread.sleep(sleepTime)
      remainingWaitMs -= sleepTime
    }
    isCleaned
  }

  /**
    * To prevent race between retention and compaction,
    * retention threads need to make this call to obtain:
    * @return A list of log partitions that retention threads can safely work on
    */
  def pauseCleaningForNonCompactedPartitions(): Iterable[(TopicPartition, Log)] = {
    cleanerManager.pauseCleaningForNonCompactedPartitions()
  }

  // Only for testing
  private[kafka] def currentConfig: CleanerConfig = config

  // Only for testing
  private[log] def cleanerCount: Int = cleaners.size

  /**
   * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
   * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
   */
  private[log] class CleanerThread(threadId: Int)
    extends ShutdownableThread(name = s"kafka-log-cleaner-thread-$threadId", isInterruptible = false) {

    protected override def loggerName = classOf[LogCleaner].getName

    if (config.dedupeBufferSize / config.numThreads > Int.MaxValue)
      warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...")

    val cleaner = new Cleaner(id = threadId,
                              offsetMap = new SkimpyOffsetMap(memory = math.min(config.dedupeBufferSize / config.numThreads, Int.MaxValue).toInt,
                                                              hashAlgorithm = config.hashAlgorithm),
                              ioBufferSize = config.ioBufferSize / config.numThreads / 2,
                              maxIoBufferSize = config.maxMessageSize,
                              dupBufferLoadFactor = config.dedupeBufferLoadFactor,
                              throttler = throttler,
                              time = time,
                              checkDone = checkDone)

    @volatile var lastStats: CleanerStats = new CleanerStats()
    @volatile var lastPreCleanStats: PreCleanStats = new PreCleanStats()

    private def checkDone(topicPartition: TopicPartition): Unit = {
      if (!isRunning)
        throw new ThreadShutdownException
      cleanerManager.checkCleaningAborted(topicPartition)
    }

    /**
     * The main loop for the cleaner thread
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
     */
    override def doWork(): Unit = {
      val cleaned = tryCleanFilthiestLog()
      if (!cleaned)
        pause(config.backOffMs, TimeUnit.MILLISECONDS)
    }

    /**
     * Cleans a log if there is a dirty log available
     * @return whether a log was cleaned
     */
    private def tryCleanFilthiestLog(): Boolean = {
      try {
        cleanFilthiestLog()
      } catch {
        case e: LogCleaningException =>
          warn(s"Unexpected exception thrown when cleaning log ${e.log}. Marking its partition (${e.log.topicPartition}) as uncleanable", e)
          cleanerManager.markPartitionUncleanable(e.log.parentDir, e.log.topicPartition)

          false
      }
    }

    @throws(classOf[LogCleaningException])
    private def cleanFilthiestLog(): Boolean = {
      val preCleanStats = new PreCleanStats()
      // 获取需要进行日志压缩的 log
      val cleaned = cleanerManager.grabFilthiestCompactedLog(time, preCleanStats) match {
        case None =>
          false
        case Some(cleanable) =>
          // there's a log, clean it
          this.lastPreCleanStats = preCleanStats
          try {
            // 核心方法 进行清理
            cleanLog(cleanable)
            true
          } catch {
            case e @ (_: ThreadShutdownException | _: ControlThrowable) => throw e
            case e: Exception => throw new LogCleaningException(cleanable.log, e.getMessage, e)
          }
      }
      // 清理待删除日志
      val deletable: Iterable[(TopicPartition, Log)] = cleanerManager.deletableLogs()
      try {
        deletable.foreach { case (_, log) =>
          try {
            // 删除日志段文件
            log.deleteOldSegments()
          } catch {
            case e @ (_: ThreadShutdownException | _: ControlThrowable) => throw e
            case e: Exception => throw new LogCleaningException(log, e.getMessage, e)
          }
        }
      } finally  {
        // 当删除任务完成后，无论是成功还是失败，都需要将该任务的状态通知给 cleanerManager，以便它可以将占用的资源释放掉。
        cleanerManager.doneDeleting(deletable.map(_._1))
      }

      cleaned
    }

    // 用来后台异步执行日志清理任务
    private def cleanLog(cleanable: LogToClean): Unit = {
      // 获取需要清理的日志文件中第一个脏数据的偏移量。
      val startOffset = cleanable.firstDirtyOffset
      // 将其赋值给需要清理的日志文件的最后一个偏移量。
      var endOffset = startOffset
      try {
        // 使用 cleaner 对指定日志文件进行清理，并返回下一个未清理的脏数据的偏移量及清理统计信息。
        val (nextDirtyOffset, cleanerStats) = cleaner.clean(cleanable)
        // 将最后一个脏数据的偏移量更新为下一个未清理的脏数据的偏移量。
        endOffset = nextDirtyOffset
        // 记录清理统计信息。
        recordStats(cleaner.id, cleanable.log.name, startOffset, endOffset, cleanerStats)
      } catch {
        case _: LogCleaningAbortedException => // task can be aborted, let it go.
        case _: KafkaStorageException => // partition is already offline. let it go.
        // 如果发生输入输出错误，则将相应的目录标记为离线，并在日志中记录错误信息。
        case e: IOException =>
          val logDirectory = cleanable.log.parentDir
          val msg = s"Failed to clean up log for ${cleanable.topicPartition} in dir $logDirectory due to IOException"
          logDirFailureChannel.maybeAddOfflineLogDir(logDirectory, msg, e)
      } finally {
        // 当清理任务完成后，无论是成功还是失败，都需要将该任务的状态通知给 cleanerManager，以便它可以将占用的资源释放掉。
        cleanerManager.doneCleaning(cleanable.topicPartition, cleanable.log.parentDirFile, endOffset)
      }
    }

    /**
     * Log out statistics on a single run of the cleaner.
     */
    def recordStats(id: Int, name: String, from: Long, to: Long, stats: CleanerStats): Unit = {
      this.lastStats = stats
      def mb(bytes: Double) = bytes / (1024*1024)
      val message =
        "%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n".format(id, name, from, to) +
        "\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n".format(mb(stats.bytesRead.toDouble),
                                                                                stats.elapsedSecs,
                                                                                mb(stats.bytesRead.toDouble / stats.elapsedSecs)) +
        "\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.mapBytesRead.toDouble),
                                                                                           stats.elapsedIndexSecs,
                                                                                           mb(stats.mapBytesRead.toDouble) / stats.elapsedIndexSecs,
                                                                                           100 * stats.elapsedIndexSecs / stats.elapsedSecs) +
        "\tBuffer utilization: %.1f%%%n".format(100 * stats.bufferUtilization) +
        "\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n".format(mb(stats.bytesRead.toDouble),
                                                                                           stats.elapsedSecs - stats.elapsedIndexSecs,
                                                                                           mb(stats.bytesRead.toDouble) / (stats.elapsedSecs - stats.elapsedIndexSecs), 100 * (stats.elapsedSecs - stats.elapsedIndexSecs) / stats.elapsedSecs) +
        "\tStart size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesRead.toDouble), stats.messagesRead) +
        "\tEnd size: %,.1f MB (%,d messages)%n".format(mb(stats.bytesWritten.toDouble), stats.messagesWritten) +
        "\t%.1f%% size reduction (%.1f%% fewer messages)%n".format(100.0 * (1.0 - stats.bytesWritten.toDouble/stats.bytesRead),
                                                                   100.0 * (1.0 - stats.messagesWritten.toDouble/stats.messagesRead))
      info(message)
      if (lastPreCleanStats.delayedPartitions > 0) {
        info("\tCleanable partitions: %d, Delayed partitions: %d, max delay: %d".format(lastPreCleanStats.cleanablePartitions, lastPreCleanStats.delayedPartitions, lastPreCleanStats.maxCompactionDelayMs))
      }
      if (stats.invalidMessagesRead > 0) {
        warn("\tFound %d invalid messages during compaction.".format(stats.invalidMessagesRead))
      }
    }

  }
}

object LogCleaner {
  val ReconfigurableConfigs = Set(
    KafkaConfig.LogCleanerThreadsProp,
    KafkaConfig.LogCleanerDedupeBufferSizeProp,
    KafkaConfig.LogCleanerDedupeBufferLoadFactorProp,
    KafkaConfig.LogCleanerIoBufferSizeProp,
    KafkaConfig.MessageMaxBytesProp,
    KafkaConfig.LogCleanerIoMaxBytesPerSecondProp,
    KafkaConfig.LogCleanerBackoffMsProp
  )

  def cleanerConfig(config: KafkaConfig): CleanerConfig = {
    CleanerConfig(numThreads = config.logCleanerThreads,
      dedupeBufferSize = config.logCleanerDedupeBufferSize,
      dedupeBufferLoadFactor = config.logCleanerDedupeBufferLoadFactor,
      ioBufferSize = config.logCleanerIoBufferSize,
      maxMessageSize = config.messageMaxBytes,
      maxIoBytesPerSecond = config.logCleanerIoMaxBytesPerSecond,
      backOffMs = config.logCleanerBackoffMs,
      enableCleaner = config.logCleanerEnable)

  }

  def createNewCleanedSegment(log: Log, baseOffset: Long): LogSegment = {
    LogSegment.deleteIfExists(log.dir, baseOffset, fileSuffix = Log.CleanedFileSuffix)
    LogSegment.open(log.dir, baseOffset, log.config, Time.SYSTEM, fileAlreadyExists = false,
      fileSuffix = Log.CleanedFileSuffix, initFileSize = log.initFileSize, preallocate = log.config.preallocate)
  }

}

/**
 * This class holds the actual logic for cleaning a log
 * @param id An identifier used for logging
 * @param offsetMap The map used for deduplication
 * @param ioBufferSize The size of the buffers to use. Memory usage will be 2x this number as there is a read and write buffer.
 * @param maxIoBufferSize The maximum size of a message that can appear in the log
 * @param dupBufferLoadFactor The maximum percent full for the deduplication buffer
 * @param throttler The throttler instance to use for limiting I/O rate.
 * @param time The time instance
 * @param checkDone Check if the cleaning for a partition is finished or aborted.
 */
// offsetMap：是一个 SkimpyOffsetMap 类型的对象，是为 dirty 部分的消息建立 key 与 last_offset 的映射关系。
// ioBufferSize： 读写 LogSegment 的 byteBuffer 的大小。
// maxIoBufferSizeSegment：消息的最大长度。
// dupBufferLoadFactor：指定了 SkimpyOffsetMap 的最大占用比率。
// throttler：用来限制读写 LogSegment 的速度。
// checkDone：检查 Log 的压缩状态。
private[log] class Cleaner(val id: Int,
                           val offsetMap: OffsetMap,
                           ioBufferSize: Int,
                           maxIoBufferSize: Int,
                           dupBufferLoadFactor: Double,
                           throttler: Throttler,
                           time: Time,
                           checkDone: TopicPartition => Unit) extends Logging {

  protected override def loggerName = classOf[LogCleaner].getName

  this.logIdent = s"Cleaner $id: "

  /* buffer used for read i/o */
  private var readBuffer = ByteBuffer.allocate(ioBufferSize)

  /* buffer used for write i/o */
  private var writeBuffer = ByteBuffer.allocate(ioBufferSize)

  private val decompressionBufferSupplier = BufferSupplier.create();

  require(offsetMap.slots * dupBufferLoadFactor > 1, "offset map is too small to fit in even a single message, so log cleaning will never make progress. You can increase log.cleaner.dedupe.buffer.size or decrease log.cleaner.threads")

  /**
   * Clean the given log
   *
   * @param cleanable The log to be cleaned
   *
   * @return The first offset not cleaned and the statistics for this round of cleaning
   */
  private[log] def clean(cleanable: LogToClean): (Long, CleanerStats) = {
    // figure out the timestamp below which it is safe to remove delete tombstones
    // this position is defined to be a configurable time beneath the last modified time of the last clean segment
    // 计算可以安全删除 ".delete" 的 logSegment (即 value 为空的消息)
    val deleteHorizonMs =
      cleanable.log.logSegments(0, cleanable.firstDirtyOffset).lastOption match {
        case None => 0L
        case Some(seg) => seg.lastModified - cleanable.log.config.deleteRetentionMs
    }

    // 执行清理工作
    doClean(cleanable, deleteHorizonMs)
  }

  private[log] def doClean(cleanable: LogToClean, deleteHorizonMs: Long): (Long, CleanerStats) = {
    info("Beginning cleaning of log %s.".format(cleanable.log.name))

    val log = cleanable.log
    // 清理状态
    val stats = new CleanerStats()

    // build the offset map
    info("Building offset map for %s...".format(cleanable.log.name))
    // 确认日志压缩的上限，因为 activeSegment不参与日志压缩，所以可以确定日志压缩的最大上限是 activeSegment.baseOffset。
    val upperBoundOffset = cleanable.firstUncleanableOffset

    // 填充 offsetMap，确定日志压缩的真正上限
    buildOffsetMap(log, cleanable.firstDirtyOffset, upperBoundOffset, offsetMap, stats)
    // 计算结束位移
    val endOffset = offsetMap.latestOffset + 1
    // 计入完成时间
    stats.indexDone()

    // determine the timestamp up to which the log will be cleaned
    // this is the lower of the last active segment and the compaction lag
    // 计算出将被清理到的时间戳，这里你可以和 upperBoundOffset 一起理解。 一个是 offset 一个是timestamp。
    val cleanableHorizonMs = log.logSegments(0, cleanable.firstUncleanableOffset).lastOption.map(_.lastModified).getOrElse(0L)

    // group the segments and clean the groups
    info("Cleaning log %s (cleaning prior to %s, discarding tombstones prior to %s)...".format(log.name, new Date(cleanableHorizonMs), new Date(deleteHorizonMs)))
    val transactionMetadata = new CleanedTransactionMetadata

    // 对要压缩的 Segment 进行分区，并且进行分组进行 clean
    val groupedSegments = groupSegmentsBySize(log.logSegments(0, endOffset), log.config.segmentSize,
      log.config.maxIndexSize, cleanable.firstUncleanableOffset)
    for (group <- groupedSegments)
      // 循环清理
      cleanSegments(log, group, offsetMap, deleteHorizonMs, stats, transactionMetadata)

    // record buffer utilization
    stats.bufferUtilization = offsetMap.utilization

    // 计入结束时间
    stats.allDone()

    (endOffset, stats)
  }

  /**
   * Clean a group of segments into a single replacement segment
   *
   * @param log The log being cleaned
   * @param segments The group of segments being cleaned
   * @param map The offset map to use for cleaning segments
   * @param deleteHorizonMs The time to retain delete tombstones
   * @param stats Collector for cleaning statistics
   * @param transactionMetadata State of ongoing transactions which is carried between the cleaning
   *                            of the grouped segments
   */
  // 将日志的多个段进行清理，并写入新的日志段中。
  private[log] def cleanSegments(log: Log,
                                 segments: Seq[LogSegment],
                                 map: OffsetMap,
                                 deleteHorizonMs: Long,
                                 stats: CleanerStats,
                                 transactionMetadata: CleanedTransactionMetadata): Unit = {
    // create a new segment with a suffix appended to the name of the log and indexes
    // 创建 ".clean" 后缀的日志文件和索引文件，文件名是分组中第一个 logSegment 的 baseOffset
    val cleaned = LogCleaner.createNewCleanedSegment(log, segments.head.baseOffset)
    // 记录已清理段的事务元数据索引 cleaned.txnIndex
    transactionMetadata.cleanedIndex = Some(cleaned.txnIndex)

    try {
      // clean segments into the new destination segment
      // 创建迭代器，迭代待清理的日志段。
      val iter = segments.iterator
      var currentSegmentOpt: Option[LogSegment] = Some(iter.next())
      val lastOffsetOfActiveProducers = log.lastRecordsOfActiveProducers

      while (currentSegmentOpt.isDefined) {
        // 获取当前日志段。
        val currentSegment = currentSegmentOpt.get
        // 获取下一个日志段（如果有的话）
        val nextSegmentOpt = if (iter.hasNext) Some(iter.next()) else None

        // 获取当前日志段的起始偏移量。
        val startOffset = currentSegment.baseOffset
        // 获取下一个日志段的起始偏移量，如果不存在下一个日志段，则取当前日志段的最大偏移量 + 1，作为上限偏移量。
        val upperBoundOffset = nextSegmentOpt.map(_.baseOffset).getOrElse(map.latestOffset + 1)
        // 获取从当前日志段的起始偏移量到上限偏移量范围内的已中止事务列表。
        val abortedTransactions = log.collectAbortedTransactions(startOffset, upperBoundOffset)
        // 将已中止事务添加到事务元数据中。
        transactionMetadata.addAbortedTransactions(abortedTransactions)

        // 根据当前日志段的修改时间戳与删除时间戳 deleteHorizonMs 的比较结果，确定是否保留删除标志和事务标记。
        // 如果当前日志段的修改时间戳晚于删除时间戳，则不进行删除。
        val retainDeletesAndTxnMarkers = currentSegment.lastModified > deleteHorizonMs
        info(s"Cleaning $currentSegment in log ${log.name} into ${cleaned.baseOffset} " +
          s"with deletion horizon $deleteHorizonMs, " +
          s"${if(retainDeletesAndTxnMarkers) "retaining" else "discarding"} deletes.")

        try {
          // 进行日志压缩操作
          cleanInto(log.topicPartition, currentSegment.log, cleaned, map, retainDeletesAndTxnMarkers, log.config.maxMessageSize,
            transactionMetadata, lastOffsetOfActiveProducers, stats)
        } catch {
          case e: LogSegmentOffsetOverflowException =>
            // Split the current segment. It's also safest to abort the current cleaning process, so that we retry from
            // scratch once the split is complete.
            info(s"Caught segment overflow error during cleaning: ${e.getMessage}")
            log.splitOverflowedSegment(currentSegment)
            throw new LogCleaningAbortedException()
        }
        // 将下一个日志段设置为当前日志段，并继续循环。
        currentSegmentOpt = nextSegmentOpt
      }

      // 标记已清理的日志段 cleaned 为不活跃的段。
      cleaned.onBecomeInactiveSegment()
      // flush new segment to disk before swap

      // 执行flush 操作，将数据刷新到磁盘上
      cleaned.flush()

      // update the modification date to retain the last modified date of the original files
      // 更新最后的修改时间
      val modified = segments.last.lastModified
      cleaned.lastModified = modified

      // swap in new segment
      info(s"Swapping in cleaned segment $cleaned for segment(s) $segments in log $log")
      // 将.clean 后缀改为 .swap 后缀
      // 将 cleaned 对象加入到 segments 中
      // 将分组中的 logSegment 从 segments 中删除
      // 最后将文件的 .swap 后缀删除
      log.replaceSegments(List(cleaned), segments)
    } catch {
      case e: LogCleaningAbortedException =>
        // 删除之前创建的新日志段 cleaned，并记录异常。
        try cleaned.deleteIfExists()
        catch {
          case deleteException: Exception =>
            e.addSuppressed(deleteException)
        } finally throw e
    }
  }

  /**
   * Clean the given source log segment into the destination segment using the key=>offset mapping
   * provided
   *
   * @param topicPartition The topic and partition of the log segment to clean
   * @param sourceRecords The dirty log segment
   * @param dest The cleaned log segment
   * @param map The key=>offset mapping
   * @param retainDeletesAndTxnMarkers Should tombstones and markers be retained while cleaning this segment
   * @param maxLogMessageSize The maximum message size of the corresponding topic
   * @param stats Collector for cleaning statistics
   */
  private[log] def cleanInto(topicPartition: TopicPartition, // 要清理的日志段所属的主题以及分区。
                             sourceRecords: FileRecords, // 源日志段，从中读取需要清理的记录。
                             dest: LogSegment, // 目标日志段，要将需要保留的记录写入其中。
                             map: OffsetMap, // 负责跟踪源日志段中记录的偏移量的
                             retainDeletesAndTxnMarkers: Boolean, // 是否在清理过程中保留删除记录和事务记录。
                             maxLogMessageSize: Int, // 日志消息的最大大小。
                             transactionMetadata: CleanedTransactionMetadata, //  清理过程中用于记录事务元数据的 CleanedTransactionMetadata。
                             lastRecordsOfActiveProducers: Map[Long, LastRecord], // 各个活跃的生产者最后一条记录的偏移量的映射表。
                             stats: CleanerStats): Unit = {  // 清理器统计数据。
    val logCleanerFilter: RecordFilter = new RecordFilter {
      var discardBatchRecords: Boolean = _

      override def checkBatchRetention(batch: RecordBatch): BatchRetention = {
        // we piggy-back on the tombstone retention logic to delay deletion of transaction markers.
        // note that we will never delete a marker until all the records from that transaction are removed.
        discardBatchRecords = shouldDiscardBatch(batch, transactionMetadata, retainTxnMarkers = retainDeletesAndTxnMarkers)

        def isBatchLastRecordOfProducer: Boolean = {
          // We retain the batch in order to preserve the state of active producers. There are three cases:
          // 1) The producer is no longer active, which means we can delete all records for that producer.
          // 2) The producer is still active and has a last data offset. We retain the batch that contains
          //    this offset since it also contains the last sequence number for this producer.
          // 3) The last entry in the log is a transaction marker. We retain this marker since it has the
          //    last producer epoch, which is needed to ensure fencing.
          lastRecordsOfActiveProducers.get(batch.producerId).exists { lastRecord =>
            lastRecord.lastDataOffset match {
              case Some(offset) => batch.lastOffset == offset
              case None => batch.isControlBatch && batch.producerEpoch == lastRecord.producerEpoch
            }
          }
        }

        if (batch.hasProducerId && isBatchLastRecordOfProducer)
          BatchRetention.RETAIN_EMPTY
        else if (discardBatchRecords)
          BatchRetention.DELETE
        else
          BatchRetention.DELETE_EMPTY
      }

      override def shouldRetainRecord(batch: RecordBatch, record: Record): Boolean = {
        // 调用上面的 checkBatchRetention 方法判断是否要丢弃。
        if (discardBatchRecords)
          // The batch is only retained to preserve producer sequence information; the records can be removed
          false
        else
        // 是否保存这个消息，有三个条件
        // 1、此消息是否含有 key
        // 2、offsetMap中是否有相同的 key
        // 3、value 不为空 或者 value 为空但是现在不可以删除
          Cleaner.this.shouldRetainRecord(map, retainDeletesAndTxnMarkers, batch, record, stats)
      }
    }

    var position = 0
    // 遍历待压缩的 LogSegment
    while (position < sourceRecords.sizeInBytes) {
      // 检查压缩状态
      checkDone(topicPartition)
      // read a chunk of messages and copy any that are to be retained to the write buffer to be written out
      readBuffer.clear()
      writeBuffer.clear()

      // 读取消息
      sourceRecords.readInto(readBuffer, position)
      val records = MemoryRecords.readableRecords(readBuffer)
      // 是否限制读取速率
      throttler.maybeThrottle(records.sizeInBytes)
      // 过滤结果
      val result = records.filterTo(topicPartition, logCleanerFilter, writeBuffer, maxLogMessageSize, decompressionBufferSupplier)
      stats.readMessages(result.messagesRead, result.bytesRead)
      stats.recopyMessages(result.messagesRetained, result.bytesRetained)

      position += result.bytesRead

      // if any messages are to be retained, write them out
      val outputBuffer = result.outputBuffer
      if (outputBuffer.position() > 0) {
        outputBuffer.flip()
        val retained = MemoryRecords.readableRecords(outputBuffer)
        // it's OK not to hold the Log's lock in this case, because this segment is only accessed by other threads
        // after `Log.replaceSegments` (which acquires the lock) is called
        // 添加到目标 logsegment 中
        dest.append(largestOffset = result.maxOffset,
          largestTimestamp = result.maxTimestamp,
          shallowOffsetOfMaxTimestamp = result.shallowOffsetOfMaxTimestamp,
          records = retained)
        throttler.maybeThrottle(outputBuffer.limit())
      }

      // if we read bytes but didn't get even one complete batch, our I/O buffer is too small, grow it and try again
      // `result.bytesRead` contains bytes from `messagesRead` and any discarded batches.
      if (readBuffer.limit() > 0 && result.bytesRead == 0)
      // 未读取一个完整的消息，表示 readBuffer 过小，需要扩容
        growBuffersOrFail(sourceRecords, position, maxLogMessageSize, records)
    }
    // 重置 readBuffer 和 writeBuffer
    restoreBuffers()
  }


  /**
   * Grow buffers to process next batch of records from `sourceRecords.` Buffers are doubled in size
   * up to a maximum of `maxLogMessageSize`. In some scenarios, a record could be bigger than the
   * current maximum size configured for the log. For example:
   *   1. A compacted topic using compression may contain a message set slightly larger than max.message.bytes
   *   2. max.message.bytes of a topic could have been reduced after writing larger messages
   * In these cases, grow the buffer to hold the next batch.
   */
  private def growBuffersOrFail(sourceRecords: FileRecords,
                                position: Int,
                                maxLogMessageSize: Int,
                                memoryRecords: MemoryRecords): Unit = {

    val maxSize = if (readBuffer.capacity >= maxLogMessageSize) {
      val nextBatchSize = memoryRecords.firstBatchSize
      val logDesc = s"log segment ${sourceRecords.file} at position $position"
      if (nextBatchSize == null)
        throw new IllegalStateException(s"Could not determine next batch size for $logDesc")
      if (nextBatchSize <= 0)
        throw new IllegalStateException(s"Invalid batch size $nextBatchSize for $logDesc")
      if (nextBatchSize <= readBuffer.capacity)
        throw new IllegalStateException(s"Batch size $nextBatchSize < buffer size ${readBuffer.capacity}, but not processed for $logDesc")
      val bytesLeft = sourceRecords.channel.size - position
      if (nextBatchSize > bytesLeft)
        throw new CorruptRecordException(s"Log segment may be corrupt, batch size $nextBatchSize > $bytesLeft bytes left in segment for $logDesc")
      nextBatchSize.intValue
    } else
      maxLogMessageSize

    growBuffers(maxSize)
  }

  private def shouldDiscardBatch(batch: RecordBatch,
                                 transactionMetadata: CleanedTransactionMetadata,
                                 retainTxnMarkers: Boolean): Boolean = {
    if (batch.isControlBatch) {
      val canDiscardControlBatch = transactionMetadata.onControlBatchRead(batch)
      canDiscardControlBatch && !retainTxnMarkers
    } else {
      val canDiscardBatch = transactionMetadata.onBatchRead(batch)
      canDiscardBatch
    }
  }

  private def shouldRetainRecord(map: kafka.log.OffsetMap,
                                 retainDeletes: Boolean,
                                 batch: RecordBatch,
                                 record: Record,
                                 stats: CleanerStats): Boolean = {
    val pastLatestOffset = record.offset > map.latestOffset
    if (pastLatestOffset)
      return true

    if (record.hasKey) {
      val key = record.key
      val foundOffset = map.get(key)
      /* First,the message must have the latest offset for the key
       * then there are two cases in which we can retain a message:
       *   1) The message has value
       *   2) The message doesn't has value but it can't be deleted now.
       */
      val latestOffsetForKey = record.offset() >= foundOffset
      val isRetainedValue = record.hasValue || retainDeletes
      latestOffsetForKey && isRetainedValue
    } else {
      stats.invalidMessage()
      false
    }
  }

  /**
   * Double the I/O buffer capacity
   */
  def growBuffers(maxLogMessageSize: Int): Unit = {
    val maxBufferSize = math.max(maxLogMessageSize, maxIoBufferSize)
    if(readBuffer.capacity >= maxBufferSize || writeBuffer.capacity >= maxBufferSize)
      throw new IllegalStateException("This log contains a message larger than maximum allowable size of %s.".format(maxBufferSize))
    val newSize = math.min(this.readBuffer.capacity * 2, maxBufferSize)
    info(s"Growing cleaner I/O buffers from ${readBuffer.capacity} bytes to $newSize bytes.")
    this.readBuffer = ByteBuffer.allocate(newSize)
    this.writeBuffer = ByteBuffer.allocate(newSize)
  }

  /**
   * Restore the I/O buffer capacity to its original size
   */
  def restoreBuffers(): Unit = {
    if(this.readBuffer.capacity > this.ioBufferSize)
      this.readBuffer = ByteBuffer.allocate(this.ioBufferSize)
    if(this.writeBuffer.capacity > this.ioBufferSize)
      this.writeBuffer = ByteBuffer.allocate(this.ioBufferSize)
  }

  /**
   * Group the segments in a log into groups totaling less than a given size. the size is enforced separately for the log data and the index data.
   * We collect a group of such segments together into a single
   * destination segment. This prevents segment sizes from shrinking too much.
   *
   * @param segments The log segments to group
   * @param maxSize the maximum size in bytes for the total of all log data in a group
   * @param maxIndexSize the maximum size in bytes for the total of all index data in a group
   *
   * @return A list of grouped segments
   */
  // 用于根据指定的最大尺寸和索引尺寸，将日志分段，并为每个段创建索引。
  private[log] def groupSegmentsBySize(segments: Iterable[LogSegment], maxSize: Int, maxIndexSize: Int, firstUncleanableOffset: Long): List[Seq[LogSegment]] = {
    // 初始化分组结果列表。
    var grouped = List[List[LogSegment]]()
    // 将迭代器转换成列表 List，以便后续处理。
    var segs = segments.toList
    // 循环处理每个日志段。
    while(segs.nonEmpty) {
      // 初始化每个日志段对应的分组列表，将当前日志段加入到列表中。
      var group = List(segs.head)
      // 初始化每个分组的字节数，将当前日志段大小加入到字节数中。
      var logSize = segs.head.size.toLong
      // 初始化每个分组索引的字节数，将当前日志段索引大小加入到索引字节数中。
      var indexSize = segs.head.offsetIndex.sizeInBytes.toLong
      // 初始化每个分组时间索引的字节数，将当前日志段时间索引大小加入到时间索引字节数中。
      var timeIndexSize = segs.head.timeIndex.sizeInBytes.toLong
      // 将当前日志段从列表中移除，以便继续处理下一个日志段。
      segs = segs.tail
      // 循环处理下一个日志段，将符合条件的日志段加入到当前分组中。
      // 条件包括：分组的总字节数未达到阈值 maxSize，分组的索引总字节数未达到阈值 maxIndexSize，
      // 分组时间索引总字节数未达到阈值maxIndexSize，当前分组的最大偏移量与待加入的日志段的起始偏移量距离不超过整型最大值。
      while(segs.nonEmpty &&
            logSize + segs.head.size <= maxSize &&
            indexSize + segs.head.offsetIndex.sizeInBytes <= maxIndexSize &&
            timeIndexSize + segs.head.timeIndex.sizeInBytes <= maxIndexSize &&
            lastOffsetForFirstSegment(segs, firstUncleanableOffset) - group.last.baseOffset <= Int.MaxValue) {
        // 将当前日志段加入到分组列表的最前面。
        group = segs.head :: group
        // 将当前日志段的大小加入到字节数中。
        logSize += segs.head.size
        // 将当前日志段的索引大小加入到索引字节数中。
        indexSize += segs.head.offsetIndex.sizeInBytes
        // 将当前日志段的时间索引大小加入到时间索引字节数中。
        timeIndexSize += segs.head.timeIndex.sizeInBytes
        // 将当前日志段从列表中移除，以便下一次继续处理。
        segs = segs.tail
      }
      // 将当前分组添加到分组列表的最前面，并倒序排列，以让最新的分组排在列表顶部。
      grouped ::= group.reverse
    }
    // 最后再将整个分组列表倒序排列，以使最旧的分组排在列表顶部。
    grouped.reverse
  }

  /**
    * We want to get the last offset in the first log segment in segs.
    * LogSegment.nextOffset() gives the exact last offset in a segment, but can be expensive since it requires
    * scanning the segment from the last index entry.
    * Therefore, we estimate the last offset of the first log segment by using
    * the base offset of the next segment in the list.
    * If the next segment doesn't exist, first Uncleanable Offset will be used.
    *
    * @param segs - remaining segments to group.
    * @return The estimated last offset for the first segment in segs
    */
  private def lastOffsetForFirstSegment(segs: List[LogSegment], firstUncleanableOffset: Long): Long = {
    if (segs.size > 1) {
      /* if there is a next segment, use its base offset as the bounding offset to guarantee we know
       * the worst case offset */
      segs(1).baseOffset - 1
    } else {
      //for the last segment in the list, use the first uncleanable offset.
      firstUncleanableOffset - 1
    }
  }

  /**
   * Build a map of key_hash => offset for the keys in the cleanable dirty portion of the log to use in cleaning.
   * @param log The log to use
   * @param start The offset at which dirty messages begin
   * @param end The ending offset for the map that is being built
   * @param map The map in which to store the mappings
   * @param stats Collector for cleaning statistics
   */
  private[log] def buildOffsetMap(log: Log,
                                  start: Long,
                                  end: Long,
                                  map: OffsetMap,
                                  stats: CleanerStats): Unit = {
    map.clear()
    // 查找从 firstDirtyOffset 至 upperBoundOffset 所有的 LogSegment
    val dirty = log.logSegments(start, end).toBuffer
    val nextSegmentStartOffsets = new ListBuffer[Long]
    if (dirty.nonEmpty) {
      for (nextSegment <- dirty.tail) nextSegmentStartOffsets.append(nextSegment.baseOffset)
      nextSegmentStartOffsets.append(end)
    }
    info("Building offset map for log %s for %d segments in offset range [%d, %d).".format(log.name, dirty.size, start, end))

    // 事务有关
    val transactionMetadata = new CleanedTransactionMetadata
    val abortedTransactions = log.collectAbortedTransactions(start, end)
    transactionMetadata.addAbortedTransactions(abortedTransactions)

    // Add all the cleanable dirty segments. We must take at least map.slots * load_factor,
    // but we may be able to fit more (if there is lots of duplication in the dirty section of the log)
    var full = false
    // 遍历 dirty 集合，循环条件是 offsetMap 未被填满
    for ((segment, nextSegmentStartOffset) <- dirty.zip(nextSegmentStartOffsets) if !full) {
      // 检查 LogCleanerManager 记录的该分区的压缩状态
      checkDone(log.topicPartition)

      // 处理单个 logSegment，将消息的 key 和 offset 添加到 OffsetMap 中
      full = buildOffsetMapForSegment(log.topicPartition, segment, map, start, nextSegmentStartOffset, log.config.maxMessageSize,
        transactionMetadata, stats)
      if (full)
        debug("Offset map is full, %d segments fully mapped, segment with base offset %d is partially mapped".format(dirty.indexOf(segment), segment.baseOffset))
    }
    info("Offset map for log %s complete.".format(log.name))
  }

  /**
   * Add the messages in the given segment to the offset map
   *
   * @param segment The segment to index
   * @param map The map in which to store the key=>offset mapping
   * @param stats Collector for cleaning statistics
   *
   * @return If the map was filled whilst loading from this segment
   */
  // 用来在给定日志段 LogSegment 中构建偏移量-键值对映射表
  private def buildOffsetMapForSegment(topicPartition: TopicPartition, // 主题分区
                                       segment: LogSegment, // 待构建映射表的日志段
                                       map: OffsetMap, // 映射表
                                       startOffset: Long, // 起始偏移量
                                       nextSegmentStartOffset: Long, // 下一日志段的起始偏移量
                                       maxLogMessageSize: Int, // 最大日志消息大小
                                       transactionMetadata: CleanedTransactionMetadata, // 已清理事务的元数据
                                       stats: CleanerStats): Boolean = {  //清理统计信息

    // 获取给定偏移量在索引中的位置。需要注意的是，offsetIndex 就是 LogSegment 类的一个成员变量，表示偏移量索引。
    // 该变量的数据类型是 OffsetIndex，保存了一组有序的偏移量向量，用于方便地查询特定偏移量所在的文件位置。
    var position = segment.offsetIndex.lookup(startOffset).position

    // 计算目标映射表大小。map.slots 表示映射表的槽数，this.dupBufferLoadFactor 是一个常量，用于设置映射表占用缓冲区空间的最大比例。
    // 两者相乘，将得到目标映射表大小。
    val maxDesiredMapSize = (map.slots * this.dupBufferLoadFactor).toInt
    // 遍历 LogSegment
    while (position < segment.log.sizeInBytes) {
      // 检查当前线程是否被中断，如果是则抛出异常 LogCleaningAbortedException。
      checkDone(topicPartition)
      // 清空读缓冲区，准备读入数据。
      readBuffer.clear()
      try {
        // 从 LogSegment 中读取数据到缓冲区 readBuffer 中。
        segment.log.readInto(readBuffer, position)
      } catch {
        case e: Exception =>
          throw new KafkaException(s"Failed to read from segment $segment of partition $topicPartition " +
            "while loading offset map", e)
      }
      // 将缓冲区 readBuffer 中的字节数据解析为多条 MemoryRecords 记录。
      val records = MemoryRecords.readableRecords(readBuffer)
      // 尝试限制数据读取速率。
      throttler.maybeThrottle(records.sizeInBytes)

      // 记录解析数据前的起始位置。
      val startPosition = position
      // 循环处理 MemoryRecords 记录中的数据包 RecordBatch。
      for (batch <- records.batches.asScala) {
        // 如果数据包是一种控制数据包，则将其提交给 transactionMetadata 处理，并记录一条清理统计信息。
        if (batch.isControlBatch) {
          transactionMetadata.onControlBatchRead(batch)
          stats.indexMessagesRead(1)
        } else {
          // 将该数据范围中所有的消息提交给 transactionMetadata 进一步处理，并记录是否存在事务被清理的标记。
          val isAborted = transactionMetadata.onBatchRead(batch)
          if (isAborted) {
            // 如果存在被终止的事务，则跳过整个数据范围，并记录一条清理统计信息
            // If the batch is aborted, do not bother populating the offset map.
            // Note that abort markers are supported in v2 and above, which means count is defined.
            stats.indexMessagesRead(batch.countOrNull)
          } else {
            // 获取当前数据包中所有消息的迭代器。
            val recordsIterator = batch.streamingIterator(decompressionBufferSupplier)
            try {
              // 按顺序处理每条消息。
              for (record <- recordsIterator.asScala) {
                // 只处理有key的消息，且该消息的偏移量如果大于等于待处理的起始偏移量 startOffset，则将其加入到映射表中。
                if (record.hasKey && record.offset >= startOffset) {
                  // 如果当前映射表大小未超过目标大小
                  if (map.size < maxDesiredMapSize)
                    map.put(record.key, record.offset)
                  else
                  // 表示映射表已经达到目标大小或超过，返回 true，表示构建偏移量映射表的过程已经结束。
                    return true
                }
                // 记录一条处理消息的统计信息。
                stats.indexMessagesRead(1)
              }
            } finally recordsIterator.close()
          }
        }

        // 记录最后一个被处理的消息偏移量，以便下一次继续处理时跳过这些已处理的消息。
        if (batch.lastOffset >= startOffset)
          map.updateLatestOffset(batch.lastOffset)
      }
      // 记录该 MemoryRecords 记录中所有消息占据的字节数。
      val bytesRead = records.validBytes
      // 更新当前读取到的数据的结束位置。
      position += bytesRead
      // 更新处理字节数量的统计信息。
      stats.indexBytesRead(bytesRead)

      // if we didn't read even one complete message, our read buffer may be too small
      // 如果 position 没有移动表示没有读取到一个完整的 message， 则对读写缓冲区进行扩容
      if(position == startPosition)
        growBuffersOrFail(segment.log, position, maxLogMessageSize, records)
    }

    // In the case of offsets gap, fast forward to latest expected offset in this segment.
    // 在处理完当前数据包中的所有消息后，执行跳过该日志段的所有空洞。
    map.updateLatestOffset(nextSegmentStartOffset - 1L)

    // 清空所有的缓冲区资源，以便后续重复使用
    restoreBuffers()
    false
  }
}

/**
  * A simple struct for collecting pre-clean stats
  */
private class PreCleanStats() {
  var maxCompactionDelayMs = 0L
  var delayedPartitions = 0
  var cleanablePartitions = 0

  def updateMaxCompactionDelay(delayMs: Long): Unit = {
    maxCompactionDelayMs = Math.max(maxCompactionDelayMs, delayMs)
    if (delayMs > 0) {
      delayedPartitions += 1
    }
  }
  def recordCleanablePartitions(numOfCleanables: Int): Unit = {
    cleanablePartitions = numOfCleanables
  }
}

/**
 * A simple struct for collecting stats about log cleaning
 */
private class CleanerStats(time: Time = Time.SYSTEM) {
  val startTime = time.milliseconds
  var mapCompleteTime = -1L
  var endTime = -1L
  var bytesRead = 0L
  var bytesWritten = 0L
  var mapBytesRead = 0L
  var mapMessagesRead = 0L
  var messagesRead = 0L
  var invalidMessagesRead = 0L
  var messagesWritten = 0L
  var bufferUtilization = 0.0d

  def readMessages(messagesRead: Int, bytesRead: Int): Unit = {
    this.messagesRead += messagesRead
    this.bytesRead += bytesRead
  }

  def invalidMessage(): Unit = {
    invalidMessagesRead += 1
  }

  def recopyMessages(messagesWritten: Int, bytesWritten: Int): Unit = {
    this.messagesWritten += messagesWritten
    this.bytesWritten += bytesWritten
  }

  def indexMessagesRead(size: Int): Unit = {
    mapMessagesRead += size
  }

  def indexBytesRead(size: Int): Unit = {
    mapBytesRead += size
  }

  def indexDone(): Unit = {
    mapCompleteTime = time.milliseconds
  }

  def allDone(): Unit = {
    endTime = time.milliseconds
  }

  def elapsedSecs: Double = (endTime - startTime) / 1000.0

  def elapsedIndexSecs: Double = (mapCompleteTime - startTime) / 1000.0

}

/**
  * Helper class for a log, its topic/partition, the first cleanable position, the first uncleanable dirty position,
  * and whether it needs compaction immediately.
  */
private case class LogToClean(topicPartition: TopicPartition,
                              log: Log,
                              firstDirtyOffset: Long,
                              uncleanableOffset: Long,
                              needCompactionNow: Boolean = false) extends Ordered[LogToClean] {
  val cleanBytes = log.logSegments(-1, firstDirtyOffset).map(_.size.toLong).sum
  val (firstUncleanableOffset, cleanableBytes) = LogCleanerManager.calculateCleanableBytes(log, firstDirtyOffset, uncleanableOffset)
  val totalBytes = cleanBytes + cleanableBytes
  val cleanableRatio = cleanableBytes / totalBytes.toDouble
  override def compare(that: LogToClean): Int = math.signum(this.cleanableRatio - that.cleanableRatio).toInt
}

/**
 * This is a helper class to facilitate tracking transaction state while cleaning the log. It maintains a set
 * of the ongoing aborted and committed transactions as the cleaner is working its way through the log. This
 * class is responsible for deciding when transaction markers can be removed and is therefore also responsible
 * for updating the cleaned transaction index accordingly.
 */
private[log] class CleanedTransactionMetadata {
  private val ongoingCommittedTxns = mutable.Set.empty[Long]
  private val ongoingAbortedTxns = mutable.Map.empty[Long, AbortedTransactionMetadata]
  // Minheap of aborted transactions sorted by the transaction first offset
  private val abortedTransactions = mutable.PriorityQueue.empty[AbortedTxn](new Ordering[AbortedTxn] {
    override def compare(x: AbortedTxn, y: AbortedTxn): Int = x.firstOffset compare y.firstOffset
  }.reverse)

  // Output cleaned index to write retained aborted transactions
  var cleanedIndex: Option[TransactionIndex] = None

  def addAbortedTransactions(abortedTransactions: List[AbortedTxn]): Unit = {
    this.abortedTransactions ++= abortedTransactions
  }

  /**
   * Update the cleaned transaction state with a control batch that has just been traversed by the cleaner.
   * Return true if the control batch can be discarded.
   */
  def onControlBatchRead(controlBatch: RecordBatch): Boolean = {
    consumeAbortedTxnsUpTo(controlBatch.lastOffset)

    val controlRecordIterator = controlBatch.iterator
    if (controlRecordIterator.hasNext) {
      val controlRecord = controlRecordIterator.next()
      val controlType = ControlRecordType.parse(controlRecord.key)
      val producerId = controlBatch.producerId
      controlType match {
        case ControlRecordType.ABORT =>
          ongoingAbortedTxns.remove(producerId) match {
            // Retain the marker until all batches from the transaction have been removed.
            // We may retain a record from an aborted transaction if it is the last entry
            // written by a given producerId.
            case Some(abortedTxnMetadata) if abortedTxnMetadata.lastObservedBatchOffset.isDefined =>
              cleanedIndex.foreach(_.append(abortedTxnMetadata.abortedTxn))
              false
            case _ => true
          }

        case ControlRecordType.COMMIT =>
          // This marker is eligible for deletion if we didn't traverse any batches from the transaction
          !ongoingCommittedTxns.remove(producerId)

        case _ => false
      }
    } else {
      // An empty control batch was already cleaned, so it's safe to discard
      true
    }
  }

  private def consumeAbortedTxnsUpTo(offset: Long): Unit = {
    while (abortedTransactions.headOption.exists(_.firstOffset <= offset)) {
      val abortedTxn = abortedTransactions.dequeue()
      ongoingAbortedTxns.getOrElseUpdate(abortedTxn.producerId, new AbortedTransactionMetadata(abortedTxn))
    }
  }

  /**
   * Update the transactional state for the incoming non-control batch. If the batch is part of
   * an aborted transaction, return true to indicate that it is safe to discard.
   */
  def onBatchRead(batch: RecordBatch): Boolean = {
    consumeAbortedTxnsUpTo(batch.lastOffset)
    if (batch.isTransactional) {
      ongoingAbortedTxns.get(batch.producerId) match {
        case Some(abortedTransactionMetadata) =>
          abortedTransactionMetadata.lastObservedBatchOffset = Some(batch.lastOffset)
          true
        case None =>
          ongoingCommittedTxns += batch.producerId
          false
      }
    } else {
      false
    }
  }

}

private class AbortedTransactionMetadata(val abortedTxn: AbortedTxn) {
  var lastObservedBatchOffset: Option[Long] = None

  override def toString: String = s"(txn: $abortedTxn, lastOffset: $lastObservedBatchOffset)"
}
