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
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import kafka.common.{KafkaException, LogCleaningAbortedException}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.LogDirFailureChannel
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils.CoreUtils._
import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.KafkaStorageException

import scala.collection.{Iterable, Seq, mutable}

private[log] sealed trait LogCleaningState
private[log] case object LogCleaningInProgress extends LogCleaningState
private[log] case object LogCleaningAborted extends LogCleaningState
private[log] case class LogCleaningPaused(pausedCount: Int) extends LogCleaningState

private[log] class LogCleaningException(val log: Log,
                                        private val message: String,
                                        private val cause: Throwable) extends KafkaException(message, cause)

/**
  * This class manages the state of each partition being cleaned.
  * LogCleaningState defines the cleaning states that a TopicPartition can be in.
  * 1. None                    : No cleaning state in a TopicPartition. In this state, it can become LogCleaningInProgress
  *                              or LogCleaningPaused(1). Valid previous state are LogCleaningInProgress and LogCleaningPaused(1)
  * 2. LogCleaningInProgress   : The cleaning is currently in progress. In this state, it can become None when log cleaning is finished
  *                              or become LogCleaningAborted. Valid previous state is None.
  * 3. LogCleaningAborted      : The cleaning abort is requested. In this state, it can become LogCleaningPaused(1).
  *                              Valid previous state is LogCleaningInProgress.
  * 4-a. LogCleaningPaused(1)  : The cleaning is paused once. No log cleaning can be done in this state.
  *                              In this state, it can become None or LogCleaningPaused(2).
  *                              Valid previous state is None, LogCleaningAborted or LogCleaningPaused(2).
  * 4-b. LogCleaningPaused(i)  : The cleaning is paused i times where i>= 2. No log cleaning can be done in this state.
  *                              In this state, it can become LogCleaningPaused(i-1) or LogCleaningPaused(i+1).
  *                              Valid previous state is LogCleaningPaused(i-1) or LogCleaningPaused(i+1).
  */
private[log] class LogCleanerManager(val logDirs: Seq[File],
                                     val logs: Pool[TopicPartition, Log],
                                     val logDirFailureChannel: LogDirFailureChannel) extends Logging with KafkaMetricsGroup {
  import LogCleanerManager._


  protected override def loggerName = classOf[LogCleaner].getName

  // package-private for testing
  private[log] val offsetCheckpointFile = "cleaner-offset-checkpoint"

  /* the offset checkpoints holding the last cleaned point for each log */
  @volatile private var checkpoints = logDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, offsetCheckpointFile), logDirFailureChannel))).toMap

  /* the set of logs currently being cleaned */
  private val inProgress = mutable.HashMap[TopicPartition, LogCleaningState]()

  /* the set of uncleanable partitions (partitions that have raised an unexpected error during cleaning)
   *   for each log directory */
  private val uncleanablePartitions = mutable.HashMap[String, mutable.Set[TopicPartition]]()

  /* a global lock used to control all access to the in-progress set and the offset checkpoints */
  private val lock = new ReentrantLock

  /* for coordinating the pausing and the cleaning of a partition */
  private val pausedCleaningCond = lock.newCondition()

  /* gauges for tracking the number of partitions marked as uncleanable for each log directory */
  for (dir <- logDirs) {
    newGauge("uncleanable-partitions-count",
      () => inLock(lock) { uncleanablePartitions.get(dir.getAbsolutePath).map(_.size).getOrElse(0) },
      Map("logDirectory" -> dir.getAbsolutePath)
    )
  }

  /* gauges for tracking the number of uncleanable bytes from uncleanable partitions for each log directory */
  for (dir <- logDirs) {
    newGauge("uncleanable-bytes",
      () => inLock(lock) {
        uncleanablePartitions.get(dir.getAbsolutePath) match {
          case Some(partitions) =>
            val lastClean = allCleanerCheckpoints
            val now = Time.SYSTEM.milliseconds
            partitions.iterator.map { tp =>
              val log = logs.get(tp)
              val lastCleanOffset = lastClean.get(tp)
              val offsetsToClean = cleanableOffsets(log, lastCleanOffset, now)
              val (_, uncleanableBytes) = calculateCleanableBytes(log, offsetsToClean.firstDirtyOffset, offsetsToClean.firstUncleanableDirtyOffset)
              uncleanableBytes
            }.sum
          case None => 0
        }
      },
      Map("logDirectory" -> dir.getAbsolutePath)
    )
  }

  /* a gauge for tracking the cleanable ratio of the dirtiest log */
  @volatile private var dirtiestLogCleanableRatio = 0.0
  newGauge("max-dirty-percent", () => (100 * dirtiestLogCleanableRatio).toInt)

  /* a gauge for tracking the time since the last log cleaner run, in milli seconds */
  @volatile private var timeOfLastRun: Long = Time.SYSTEM.milliseconds
  newGauge("time-since-last-run-ms", () => Time.SYSTEM.milliseconds - timeOfLastRun)

  /**
   * @return the position processed for all logs.
   */
  def allCleanerCheckpoints: Map[TopicPartition, Long] = {
    inLock(lock) {
      checkpoints.values.flatMap(checkpoint => {
        try {
          checkpoint.read()
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
            Map.empty[TopicPartition, Long]
        }
      }).toMap
    }
  }

  /**
    * Package private for unit test. Get the cleaning state of the partition.
    */
  private[log] def cleaningState(tp: TopicPartition): Option[LogCleaningState] = {
    inLock(lock) {
      inProgress.get(tp)
    }
  }

  /**
    * Package private for unit test. Set the cleaning state of the partition.
    */
  private[log] def setCleaningState(tp: TopicPartition, state: LogCleaningState): Unit = {
    inLock(lock) {
      inProgress.put(tp, state)
    }
  }

   /**
    * Choose the log to clean next and add it to the in-progress set. We recompute this
    * each time from the full set of logs to allow logs to be dynamically added to the pool of logs
    * the log manager maintains.
    */
  def grabFilthiestCompactedLog(time: Time, preCleanStats: PreCleanStats = new PreCleanStats()): Option[LogToClean] = {
    inLock(lock) {
      // 当前时间
      val now = time.milliseconds
      // 监控指标 记录 cleaner 线程跑的时间
      this.timeOfLastRun = now
      val lastClean = allCleanerCheckpoints
      // 计算脏日志信息
      val dirtyLogs = logs.filter {
        // 过滤掉 cleaner.policy 不是 compact 的 log
        case (_, log) => log.config.compact  // match logs that are marked as compacted
      }.filterNot {
        // 过滤掉已经 in-progress 集合中的不需要清理的 log
        case (topicPartition, log) =>
          // skip any logs already in-progress and uncleanable partitions
          inProgress.contains(topicPartition) || isUncleanablePartition(log, topicPartition)
      }.map {
        case (topicPartition, log) => // create a LogToClean instance for each
          try {
            val lastCleanOffset = lastClean.get(topicPartition)
            // 计算出 firstDirtyOffset，firstDirtyOffset 的值可能是 logStartOffset 也可能是 clean checkpoint
            val offsetsToClean = cleanableOffsets(log, lastCleanOffset, now)
            // update checkpoint for logs with invalid checkpointed offsets
            if (offsetsToClean.forceUpdateCheckpoint)
              // 如果需要强制更新检查点文件则进行更新
              updateCheckpoints(log.parentDirFile, partitionToUpdateOrAdd = Option(topicPartition, offsetsToClean.firstDirtyOffset))
            val compactionDelayMs = maxCompactionDelay(log, offsetsToClean.firstDirtyOffset, now)
            preCleanStats.updateMaxCompactionDelay(compactionDelayMs)
            // 为每个 Log 创建一个 LogToClean 对象，维护了每个 Log 的 clean 部分的字节数、dirty部分字节数 以及 cleanableRatio。
            LogToClean(topicPartition, log, offsetsToClean.firstDirtyOffset, offsetsToClean.firstUncleanableDirtyOffset, compactionDelayMs > 0)
          } catch {
            case e: Throwable => throw new LogCleaningException(log,
              s"Failed to calculate log cleaning stats for partition $topicPartition", e)
          }
      }.filter(ltc => ltc.totalBytes > 0) // skip any empty logs

      // 获取 dirtyLogs 集合中 cleanableRatio 的最大值
      this.dirtiestLogCleanableRatio = if (dirtyLogs.nonEmpty) dirtyLogs.max.cleanableRatio else 0
      // and must meet the minimum threshold for dirty byte ratio or have some bytes required to be compacted
      // 过滤掉 cleanableRatio 小于配置的 log
      val cleanableLogs = dirtyLogs.filter { ltc =>
        (ltc.needCompactionNow && ltc.cleanableBytes > 0) || ltc.cleanableRatio > ltc.log.config.minCleanableRatio
      }
      if(cleanableLogs.isEmpty) {
        None
      } else {
        // 选择要压缩的日志，加入 inProgress 集合中
        preCleanStats.recordCleanablePartitions(cleanableLogs.size)
        val filthiest = cleanableLogs.max
        inProgress.put(filthiest.topicPartition, LogCleaningInProgress)
        Some(filthiest)
      }
    }
  }

  /**
    * Pause logs cleaning for logs that do not have compaction enabled
    * and do not have other deletion or compaction in progress.
    * This is to handle potential race between retention and cleaner threads when users
    * switch topic configuration between compacted and non-compacted topic.
    * @return retention logs that have log cleaning successfully paused
    */
  // 暂停非压缩分区的日志清理过程
  def pauseCleaningForNonCompactedPartitions(): Iterable[(TopicPartition, Log)] = {
    inLock(lock) {
      // 筛选所有未在进行清理并且未被压缩的日志文件，返回它们的键值对。
      val deletableLogs = logs.filter {
        // 选出不需要压缩的日志文件
        case (_, log) => !log.config.compact // pick non-compacted logs
      }.filterNot {
        // 选出尚未处于日志清理过程中的文件。
        case (topicPartition, _) => inProgress.contains(topicPartition) // skip any logs already in-progress
      }

      deletableLogs.foreach {
        // 将分区和对应的暂停清理标志 LogCleaningPaused 存储到 inProgress 集合中
        case (topicPartition, _) => inProgress.put(topicPartition, LogCleaningPaused(1))
      }
      // 被筛选出的可以暂停清理的日志文件及其对应的主题分区
      deletableLogs
    }
  }

  /**
    * Find any logs that have compaction enabled. Mark them as being cleaned
    * Include logs without delete enabled, as they may have segments
    * that precede the start offset.
    */
  def deletableLogs(): Iterable[(TopicPartition, Log)] = {
    inLock(lock) {
      val toClean = logs.filter { case (topicPartition, log) =>
        !inProgress.contains(topicPartition) && log.config.compact &&
          !isUncleanablePartition(log, topicPartition)
      }
      toClean.foreach { case (tp, _) => inProgress.put(tp, LogCleaningInProgress) }
      toClean
    }

  }

  /**
   *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
   *  the partition is aborted.
   *  This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
   */
  def abortCleaning(topicPartition: TopicPartition): Unit = {
    inLock(lock) {
      abortAndPauseCleaning(topicPartition)
      resumeCleaning(Seq(topicPartition))
    }
  }

  /**
   *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
   *  This call blocks until the cleaning of the partition is aborted and paused.
   *  1. If the partition is not in progress, mark it as paused.
   *  2. Otherwise, first mark the state of the partition as aborted.
   *  3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
   *     throws a LogCleaningAbortedException to stop the cleaning task.
   *  4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
   *  5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
   *  6. If the partition is already paused, a new call to this function
   *     will increase the paused count by one.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case None =>
          inProgress.put(topicPartition, LogCleaningPaused(1))
        case Some(LogCleaningInProgress) =>
          inProgress.put(topicPartition, LogCleaningAborted)
        case Some(LogCleaningPaused(count)) =>
          inProgress.put(topicPartition, LogCleaningPaused(count + 1))
        case Some(s) =>
          throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be aborted and paused since it is in $s state.")
      }
      while(!isCleaningInStatePaused(topicPartition))
        pausedCleaningCond.await(100, TimeUnit.MILLISECONDS)
    }
  }

  /**
    *  Resume the cleaning of paused partitions.
    *  Each call of this function will undo one pause.
    */
  def resumeCleaning(topicPartitions: Iterable[TopicPartition]): Unit = {
    inLock(lock) {
      topicPartitions.foreach {
        topicPartition =>
          inProgress.get(topicPartition) match {
            case None =>
              throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is not paused.")
            case Some(state) =>
              state match {
                case LogCleaningPaused(count) if count == 1 =>
                  inProgress.remove(topicPartition)
                case LogCleaningPaused(count) if count > 1 =>
                  inProgress.put(topicPartition, LogCleaningPaused(count - 1))
                case s =>
                  throw new IllegalStateException(s"Compaction for partition $topicPartition cannot be resumed since it is in $s state.")
              }
          }
      }
    }
  }

  /**
   *  Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
   */
  private def isCleaningInState(topicPartition: TopicPartition, expectedState: LogCleaningState): Boolean = {
    inProgress.get(topicPartition) match {
      case None => false
      case Some(state) =>
        if (state == expectedState)
          true
        else
          false
    }
  }

  /**
   *  Check if the cleaning for a partition is paused. The caller is expected to hold lock while making the call.
   */
  private def isCleaningInStatePaused(topicPartition: TopicPartition): Boolean = {
    inProgress.get(topicPartition) match {
      case None => false
      case Some(state) =>
        state match {
          case _: LogCleaningPaused =>
            true
          case _ =>
            false
        }
    }
  }

  /**
   *  Check if the cleaning for a partition is aborted. If so, throw an exception.
   */
  def checkCleaningAborted(topicPartition: TopicPartition): Unit = {
    inLock(lock) {
      if (isCleaningInState(topicPartition, LogCleaningAborted))
        throw new LogCleaningAbortedException()
    }
  }

  /**
   * Update checkpoint file, adding or removing partitions if necessary.
   *
   * @param dataDir                       The File object to be updated
   * @param partitionToUpdateOrAdd        The [TopicPartition, Long] map data to be updated. pass "none" if doing remove, not add
   * @param topicPartitionToBeRemoved     The TopicPartition to be removed
   */
  def updateCheckpoints(dataDir: File, partitionToUpdateOrAdd: Option[(TopicPartition, Long)] = None,
                        partitionToRemove: Option[TopicPartition] = None): Unit = {
    inLock(lock) {
      val checkpoint = checkpoints(dataDir)
      if (checkpoint != null) {
        try {
          val currentCheckpoint = checkpoint.read().filter { case (tp, _) => logs.keys.contains(tp) }.toMap
          // remove the partition offset if any
          var updatedCheckpoint = partitionToRemove match {
            case Some(topicPartion) => currentCheckpoint - topicPartion
            case None => currentCheckpoint
          }
          // update or add the partition offset if any
          updatedCheckpoint = partitionToUpdateOrAdd match {
            case Some(updatedOffset) => updatedCheckpoint + updatedOffset
            case None => updatedCheckpoint
          }

          checkpoint.write(updatedCheckpoint)
        } catch {
          case e: KafkaStorageException =>
            error(s"Failed to access checkpoint file ${checkpoint.file.getName} in dir ${checkpoint.file.getParentFile.getAbsolutePath}", e)
        }
      }
    }
  }

  /**
   * alter the checkpoint directory for the topicPartition, to remove the data in sourceLogDir, and add the data in destLogDir
   */
  def alterCheckpointDir(topicPartition: TopicPartition, sourceLogDir: File, destLogDir: File): Unit = {
    inLock(lock) {
      try {
        checkpoints.get(sourceLogDir).flatMap(_.read().get(topicPartition)) match {
          case Some(offset) =>
            debug(s"Removing the partition offset data in checkpoint file for '${topicPartition}' " +
              s"from ${sourceLogDir.getAbsoluteFile} directory.")
            updateCheckpoints(sourceLogDir, partitionToRemove = Option(topicPartition))

            debug(s"Adding the partition offset data in checkpoint file for '${topicPartition}' " +
              s"to ${destLogDir.getAbsoluteFile} directory.")
            updateCheckpoints(destLogDir, partitionToUpdateOrAdd = Option(topicPartition, offset))
          case None =>
        }
      } catch {
        case e: KafkaStorageException =>
          error(s"Failed to access checkpoint file in dir ${sourceLogDir.getAbsolutePath}", e)
      }

      val logUncleanablePartitions = uncleanablePartitions.getOrElse(sourceLogDir.toString, mutable.Set[TopicPartition]())
      if (logUncleanablePartitions.contains(topicPartition)) {
        logUncleanablePartitions.remove(topicPartition)
        markPartitionUncleanable(destLogDir.toString, topicPartition)
      }
    }
  }

  /**
   * Stop cleaning logs in the provided directory
   *
   * @param dir     the absolute path of the log dir
   */
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping cleaning logs in dir $dir")
    inLock(lock) {
      checkpoints = checkpoints.filter { case (k, _) => k.getAbsolutePath != dir }
    }
  }

  /**
   * Truncate the checkpointed offset for the given partition if its checkpointed offset is larger than the given offset
   */
  def maybeTruncateCheckpoint(dataDir: File, topicPartition: TopicPartition, offset: Long): Unit = {
    inLock(lock) {
      if (logs.get(topicPartition).config.compact) {
        val checkpoint = checkpoints(dataDir)
        if (checkpoint != null) {
          val existing = checkpoint.read()
          if (existing.getOrElse(topicPartition, 0L) > offset)
            checkpoint.write(mutable.Map() ++= existing += topicPartition -> offset)
        }
      }
    }
  }

  /**
   * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
   */
  def doneCleaning(topicPartition: TopicPartition, dataDir: File, endOffset: Long): Unit = {
    inLock(lock) {
      inProgress.get(topicPartition) match {
        case Some(LogCleaningInProgress) =>
          updateCheckpoints(dataDir, partitionToUpdateOrAdd = Option(topicPartition, endOffset))
          inProgress.remove(topicPartition)
        case Some(LogCleaningAborted) =>
          inProgress.put(topicPartition, LogCleaningPaused(1))
          pausedCleaningCond.signalAll()
        case None =>
          throw new IllegalStateException(s"State for partition $topicPartition should exist.")
        case s =>
          throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
      }
    }
  }

  def doneDeleting(topicPartitions: Iterable[TopicPartition]): Unit = {
    inLock(lock) {
      topicPartitions.foreach {
        topicPartition =>
          inProgress.get(topicPartition) match {
            case Some(LogCleaningInProgress) =>
              inProgress.remove(topicPartition)
            case Some(LogCleaningAborted) =>
              inProgress.put(topicPartition, LogCleaningPaused(1))
              pausedCleaningCond.signalAll()
            case None =>
              throw new IllegalStateException(s"State for partition $topicPartition should exist.")
            case s =>
              throw new IllegalStateException(s"In-progress partition $topicPartition cannot be in $s state.")
          }
      }
    }
  }

  /**
   * Returns an immutable set of the uncleanable partitions for a given log directory
   * Only used for testing
   */
  private[log] def uncleanablePartitions(logDir: String): Set[TopicPartition] = {
    var partitions: Set[TopicPartition] = Set()
    inLock(lock) { partitions ++= uncleanablePartitions.getOrElse(logDir, partitions) }
    partitions
  }

  def markPartitionUncleanable(logDir: String, partition: TopicPartition): Unit = {
    inLock(lock) {
      uncleanablePartitions.get(logDir) match {
        case Some(partitions) =>
          partitions.add(partition)
        case None =>
          uncleanablePartitions.put(logDir, mutable.Set(partition))
      }
    }
  }

  private def isUncleanablePartition(log: Log, topicPartition: TopicPartition): Boolean = {
    inLock(lock) {
      uncleanablePartitions.get(log.parentDir).exists(partitions => partitions.contains(topicPartition))
    }
  }
}

/**
 * Helper class for the range of cleanable dirty offsets of a log and whether to update the checkpoint associated with
 * the log
 *
 * @param firstDirtyOffset the lower (inclusive) offset to begin cleaning from
 * @param firstUncleanableDirtyOffset the upper(exclusive) offset to clean to
 * @param forceUpdateCheckpoint whether to update the checkpoint associated with this log. if true, checkpoint should be
 *                             reset to firstDirtyOffset
 */
private case class OffsetsToClean(firstDirtyOffset: Long,
                                  firstUncleanableDirtyOffset: Long,
                                  forceUpdateCheckpoint: Boolean = false) {
}

private[log] object LogCleanerManager extends Logging {

  def isCompactAndDelete(log: Log): Boolean = {
    log.config.compact && log.config.delete
  }

  /**
    * get max delay between the time when log is required to be compacted as determined
    * by maxCompactionLagMs and the current time.
    */
  def maxCompactionDelay(log: Log, firstDirtyOffset: Long, now: Long) : Long = {
    val dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset)
    val firstBatchTimestamps = log.getFirstBatchTimestampForSegments(dirtyNonActiveSegments).filter(_ > 0)

    val earliestDirtySegmentTimestamp = {
      if (firstBatchTimestamps.nonEmpty)
        firstBatchTimestamps.min
      else Long.MaxValue
    }

    val maxCompactionLagMs = math.max(log.config.maxCompactionLagMs, 0L)
    val cleanUntilTime = now - maxCompactionLagMs

    if (earliestDirtySegmentTimestamp < cleanUntilTime)
      cleanUntilTime - earliestDirtySegmentTimestamp
    else
      0L
  }

  /**
    * Returns the range of dirty offsets that can be cleaned.
    *
    * @param log the log
    * @param lastCleanOffset the last checkpointed offset
    * @param now the current time in milliseconds of the cleaning operation
    * @return OffsetsToClean containing offsets for cleanable portion of log and whether the log checkpoint needs updating
    */

  // 用于获取可以清理的日志文件偏移量范围。
  // 三个参数：需要清理的日志文件 log，最后一次清理的偏移量 lastCleanOffset，以及当前时间戳 now
  def cleanableOffsets(log: Log, lastCleanOffset: Option[Long], now: Long): OffsetsToClean = {
    // If the log segments are abnormally truncated and hence the checkpointed offset is no longer valid;
    // reset to the log starting offset and log the error
    // 获取第一个脏数据的偏移量 firstDirtyOffset 及是否需要更新检查点标识 forceUpdateCheckpoint。
    val (firstDirtyOffset, forceUpdateCheckpoint) = {
      // 获取日志文件的起始偏移量。
      val logStartOffset = log.logStartOffset
      // 获取上一次清理结束时所记录的偏移量。如果该值不存在，则使用日志文件的起始偏移量。
      val checkpointDirtyOffset = lastCleanOffset.getOrElse(logStartOffset)

      // 如果上一次清理结束时记录的值小于日志文件的起始偏移量，则认为检查点无效，重置第一个脏数据的偏移量为起始偏移量。
      if (checkpointDirtyOffset < logStartOffset) {
        // Don't bother with the warning if compact and delete are enabled.
        if (!isCompactAndDelete(log))
          warn(s"Resetting first dirty offset of ${log.name} to log start offset $logStartOffset " +
            s"since the checkpointed offset $checkpointDirtyOffset is invalid.")
        (logStartOffset, true)

        // 如果上一次清理结束时记录的值大于日志文件的结尾偏移量，则暂时认为整个日志文件需要被清理。
      } else if (checkpointDirtyOffset > log.logEndOffset) {
        // The dirty offset has gotten ahead of the log end offset. This could happen if there was data
        // corruption at the end of the log. We conservatively assume that the full log needs cleaning.
        warn(s"The last checkpoint dirty offset for partition ${log.name} is $checkpointDirtyOffset, " +
          s"which is larger than the log end offset ${log.logEndOffset}. Resetting to the log start offset $logStartOffset.")
        (logStartOffset, true)
      } else {
        // 否则，使用上一次清理结束时的偏移量作为第一个脏数据的偏移量。
        (checkpointDirtyOffset, false)
      }
    }

    // 获取最小压缩延迟时间（得到配置文件中更大的数和0，防止小于0）。
    val minCompactionLagMs = math.max(log.config.compactionLagMs, 0L)

    // find first segment that cannot be cleaned
    // neither the active segment, nor segments with any messages closer to the head of the log than the minimum compaction lag time
    // may be cleaned
    // 创建一个序列，包含了需要跳过的日志段。
    val firstUncleanableDirtyOffset: Long = Seq(

      // we do not clean beyond the first unstable offset
      // 首先，跳过第一个不稳定的偏移量。
      log.firstUnstableOffset,

      // the active segment is always uncleanable
      // 其次，跳过当前活跃日志段。
      Option(log.activeSegment.baseOffset),

      // the first segment whose largest message timestamp is within a minimum time lag from now
      // 如果最小压缩延迟时间大于零，则需要跳过具有时间戳在最小压缩延迟时间之内的脏数据段。
      if (minCompactionLagMs > 0) {
        // dirty log segments
        // 获取从第一个脏数据偏移量开始的所有脏数据日志段。
        val dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset)
        // 在脏数据日志段中查找第一个其最大消息时间戳小于当前时间减去最小压缩延迟时间的日志段，然后将其基础偏移量返回。
        // 如果不存在这样的日志段，则返回 None 值。
        dirtyNonActiveSegments.find { s =>
          val isUncleanable = s.largestTimestamp > now - minCompactionLagMs
          debug(s"Checking if log segment may be cleaned: log='${log.name}' segment.baseOffset=${s.baseOffset} " +
            s"segment.largestTimestamp=${s.largestTimestamp}; now - compactionLag=${now - minCompactionLagMs}; " +
            s"is uncleanable=$isUncleanable")
          isUncleanable
        }.map(_.baseOffset)
      } else None
    ).flatten.min  // 如果存在多个需要跳过的日志段，选择最小基础偏移量的日志

    debug(s"Finding range of cleanable offsets for log=${log.name}. Last clean offset=$lastCleanOffset " +
      s"now=$now => firstDirtyOffset=$firstDirtyOffset firstUncleanableOffset=$firstUncleanableDirtyOffset " +
      s"activeSegment.baseOffset=${log.activeSegment.baseOffset}")

    // 创建一个 OffsetsToClean 对象，封装了可以清理的偏移量范围并返回
    OffsetsToClean(firstDirtyOffset, math.max(firstDirtyOffset, firstUncleanableDirtyOffset), forceUpdateCheckpoint)
  }

  /**
   * Given the first dirty offset and an uncleanable offset, calculates the total cleanable bytes for this log
   * @return the biggest uncleanable offset and the total amount of cleanable bytes
   */
  def calculateCleanableBytes(log: Log, firstDirtyOffset: Long, uncleanableOffset: Long): (Long, Long) = {
    val firstUncleanableSegment = log.nonActiveLogSegmentsFrom(uncleanableOffset).headOption.getOrElse(log.activeSegment)
    val firstUncleanableOffset = firstUncleanableSegment.baseOffset
    val cleanableBytes = log.logSegments(math.min(firstDirtyOffset, firstUncleanableOffset), firstUncleanableOffset).map(_.size.toLong).sum

    (firstUncleanableOffset, cleanableBytes)
  }

}
