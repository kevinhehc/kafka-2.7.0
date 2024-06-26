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

import java.io._
import java.nio.file.Files
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import kafka.metrics.KafkaMetricsGroup
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.server.{BrokerState, RecoveringFromUncleanShutdown, _}
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.errors.{KafkaStorageException, LogDirNotFoundException}

import scala.jdk.CollectionConverters._
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 *
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 *
 * A background thread handles log retention by periodically truncating excess log segments.
 */
// 其中周期性任务包括以下 4 个任务：
//  1、日志刷盘任务：kafka-log-flusher。
//  2、日志保留任务：log-retention。
//  3、日志检查点刷新任务：kafka-recovery-point-checkpoint、kafka-log-start-offset-checkpoint。
//  4、日志清理任务：kafka-delete-logs。
@threadsafe
class LogManager(logDirs: Seq[File],
                 initialOfflineDirs: Seq[File],
                 val topicConfigs: Map[String, LogConfig], // note that this doesn't get updated after creation
                 // 一组主题和其对应的日志配置。注：此配置只在创建 LogManager 时使用，创建后不再更新。

                 val initialDefaultConfig: LogConfig,  // 默认的日志配置
                 val cleanerConfig: CleanerConfig, // 清理策略相关配置
                 recoveryThreadsPerDataDir: Int, // 恢复线程数
                 val flushCheckMs: Long, // 日志刷盘间隔时间（毫秒）
                 val flushRecoveryOffsetCheckpointMs: Long, // 恢复偏移量检查点刷盘间隔时间（毫秒）
                 val flushStartOffsetCheckpointMs: Long,  // 日志起始偏移量检查点刷盘间隔时间（毫秒）
                 val retentionCheckMs: Long,  // 日志保留时间检查间隔时间（毫秒）
                 val maxPidExpirationMs: Int,  // 事务分区id的有效时间
                 scheduler: Scheduler, // 调度器
                 val brokerState: BrokerState,  // broker状态
                 brokerTopicStats: BrokerTopicStats, // broker主题状态
                 logDirFailureChannel: LogDirFailureChannel,  // 日志目录错误信息处理器
                 time: Time) extends Logging with KafkaMetricsGroup {

  import LogManager._

  val LockFile = ".lock"
  val InitialTaskDelayMs = 30 * 1000 // 日志从磁盘加载和初始化的延迟时间（毫秒）

  private val logCreationOrDeletionLock = new Object // 创建或删除 Log 时的锁对象
  private val currentLogs = new Pool[TopicPartition, Log]() // 当前正在使用的日志池
  // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
  // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
  // to replace the current log of the partition after the future log catches up with the current log
  private val futureLogs = new Pool[TopicPartition, Log]()  // 预备日志池
  // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
  private val logsToBeDeleted = new LinkedBlockingQueue[(Log, Long)]() // 待删除的日志队列

  // 检查日志目录
  private val _liveLogDirs: ConcurrentLinkedQueue[File] = createAndValidateLogDirs(logDirs, initialOfflineDirs)
  // 当前默认日志配置对象
  @volatile private var _currentDefaultConfig = initialDefaultConfig
  // 恢复线程数
  @volatile private var numRecoveryThreadsPerDataDir = recoveryThreadsPerDataDir

  // This map contains all partitions whose logs are getting loaded and initialized. If log configuration
  // of these partitions get updated at the same time, the corresponding entry in this map is set to "true",
  // which triggers a config reload after initialization is finished (to get the latest config value).
  // See KAFKA-8813 for more detail on the race condition
  // Visible for testing
  // 持有正在加载和初始化的分区对象，并且在配置更新后触发重新加载。参见KAFKA-8813以了解更多详情。
  private[log] val partitionsInitializing = new ConcurrentHashMap[TopicPartition, Boolean]().asScala

  // 更新默认配置对象
  def reconfigureDefaultLogConfig(logConfig: LogConfig): Unit = {
    this._currentDefaultConfig = logConfig
  }

  // 获取当前默认配置对象
  def currentDefaultConfig: LogConfig = _currentDefaultConfig

  // 获取当前所有的在线日志目录
  def liveLogDirs: Seq[File] = {
    if (_liveLogDirs.size == logDirs.size)
      logDirs
    else
      _liveLogDirs.asScala.toBuffer
  }

  // 用于锁定每个目录结构的文件
  private val dirLocks = lockLogDirs(liveLogDirs)
  // 每个数据目录都有一个检查点文件,存储这个数据目录下所有分区的检查点信息
  @volatile private var recoveryPointCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, RecoveryPointCheckpointFile), logDirFailureChannel))).toMap
  // 在日志目录中存储日志起始偏移量的文件名
  @volatile private var logStartOffsetCheckpoints = liveLogDirs.map(dir =>
    (dir, new OffsetCheckpointFile(new File(dir, LogStartOffsetCheckpointFile), logDirFailureChannel))).toMap

  // 用于记录哪些分区应该在清理时被放置在哪个日志目录中
  private val preferredLogDirs = new ConcurrentHashMap[TopicPartition, String]()

  private def offlineLogDirs: Iterable[File] = {
    val logDirsSet = mutable.Set[File]() ++= logDirs
    _liveLogDirs.forEach(dir => logDirsSet -= dir)
    logDirsSet
  }

  // 加载所有的日志
  loadLogs()

  // 如果启用Cleaner，则创建LogCleaner对象，否则设为null
  private[kafka] val cleaner: LogCleaner =
    if (cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, liveLogDirs, currentLogs, logDirFailureChannel, time = time)
    else
      null

  // 监控已下线的日志目录数量
  newGauge("OfflineLogDirectoryCount", () => offlineLogDirs.size)

  // 监控日志目录的离线状态，并将其信息输出到 Graphite，以便后续监控使用
  for (dir <- logDirs) {
    newGauge("LogDirectoryOffline",
      () => if (_liveLogDirs.contains(dir)) 0 else 1,
      Map("logDirectory" -> dir.getAbsolutePath))
  }

  /**
   * Create and check validity of the given directories that are not in the given offline directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory
   * </ol>
   */
  // 用于创建并验证日志目录的合法性
  private def createAndValidateLogDirs(dirs: Seq[File], initialOfflineDirs: Seq[File]): ConcurrentLinkedQueue[File] = {
    // 初始化当前存在的日志目录集合，创建 ConcurrentLinkedQueue 对象，用于保存可用的日志目录
    val liveLogDirs = new ConcurrentLinkedQueue[File]()
    // 用于保存日志目录的规范路径
    val canonicalPaths = mutable.HashSet.empty[String]

    // 遍历所有传入的日志目录 如：log.dirs=/tmp/kafka-logs
    for (dir <- dirs) {
      try {
        // liveLogDirs与offline日志目录不能有重叠情况，否则抛异常
        if (initialOfflineDirs.contains(dir))
          throw new IOException(s"Failed to load ${dir.getAbsolutePath} during broker startup")

        // 如果日志目录不存在，则尝试创建目录
        if (!dir.exists) {
          info(s"Log directory ${dir.getAbsolutePath} not found, creating it.")
          // 创建目录
          val created = dir.mkdirs()
          if (!created)
            throw new IOException(s"Failed to create data directory ${dir.getAbsolutePath}")
        }
        // 确保每个日志目录是个目录且有可读权限
        if (!dir.isDirectory || !dir.canRead)
          throw new IOException(s"${dir.getAbsolutePath} is not a readable log directory.")

        // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
        // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
        // and mark the log directory as offline.
        // 获取目录的规范路径。
        // 如果给出的 File 对象表示的路径包含符号链接，则此方法会解析符号链接后返回路径名
        // 注意，如果一个文件系统查询失败或路径是无效的（例如包含空字符'\0'），那么这个方法就会抛出 IOException 异常。
        if (!canonicalPaths.add(dir.getCanonicalPath))
          throw new KafkaException(s"Duplicate log directory found: ${dirs.mkString(", ")}")


        // 把创建好的日志目录加到集合里。
        liveLogDirs.add(dir)
      } catch {
        // 如果出现异常，则将该目录添加到通道，以便将来进行重试
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Failed to create or validate data directory ${dir.getAbsolutePath}", e)
      }
    }

    // 如果没有可用的目录，则打印错误日志，退出程序
    if (liveLogDirs.isEmpty) {
      fatal(s"Shutdown broker because none of the specified log dirs from ${dirs.mkString(", ")} can be created or validated")
      Exit.halt(1)
    }

    // 返回可用目录的 liveLogDirs 集合
    liveLogDirs
  }

  def resizeRecoveryThreadPool(newSize: Int): Unit = {
    info(s"Resizing recovery thread pool size for each data dir from $numRecoveryThreadsPerDataDir to $newSize")
    numRecoveryThreadsPerDataDir = newSize
  }

  /**
   * The log directory failure handler. It will stop log cleaning in that directory.
   *
   * @param dir        the absolute path of the log directory
   */
  def handleLogDirFailure(dir: String): Unit = {
    warn(s"Stopping serving logs in dir $dir")
    logCreationOrDeletionLock synchronized {
      _liveLogDirs.remove(new File(dir))
      if (_liveLogDirs.isEmpty) {
        fatal(s"Shutdown broker because all log dirs in ${logDirs.mkString(", ")} have failed")
        Exit.halt(1)
      }

      recoveryPointCheckpoints = recoveryPointCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      logStartOffsetCheckpoints = logStartOffsetCheckpoints.filter { case (file, _) => file.getAbsolutePath != dir }
      if (cleaner != null)
        cleaner.handleLogDirFailure(dir)

      def removeOfflineLogs(logs: Pool[TopicPartition, Log]): Iterable[TopicPartition] = {
        val offlineTopicPartitions: Iterable[TopicPartition] = logs.collect {
          case (tp, log) if log.parentDir == dir => tp
        }
        offlineTopicPartitions.foreach { topicPartition => {
          val removedLog = removeLogAndMetrics(logs, topicPartition)
          removedLog.foreach {
            log => log.closeHandlers()
          }
        }}

        offlineTopicPartitions
      }

      val offlineCurrentTopicPartitions = removeOfflineLogs(currentLogs)
      val offlineFutureTopicPartitions = removeOfflineLogs(futureLogs)

      warn(s"Logs for partitions ${offlineCurrentTopicPartitions.mkString(",")} are offline and " +
           s"logs for future partitions ${offlineFutureTopicPartitions.mkString(",")} are offline due to failure on log directory $dir")
      dirLocks.filter(_.file.getParent == dir).foreach(dir => CoreUtils.swallow(dir.destroy(), this))
    }
  }

  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    // 对所有的目录进行遍历和处理
    dirs.flatMap { dir =>
      try {
        // 在每个目录中创建文件锁对象 FileLock
        val lock = new FileLock(new File(dir, LockFile))
        // 尝试获取锁资源，如果成功，则返回 Some(lock)
        if (!lock.tryLock())
          throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParent +
            ". A Kafka instance in another process or thread is using this directory.")
        Some(lock)
      } catch {
        // 如果出现异常，则将该目录的异常添加到通道，以便将来进行重试
        case e: IOException =>
          logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath, s"Disk error while locking directory $dir", e)
          None
      }
    }
  }

  private def addLogToBeDeleted(log: Log): Unit = {
    this.logsToBeDeleted.add((log, time.milliseconds()))
  }

  // Only for testing
  private[log] def hasLogsToBeDeleted: Boolean = !logsToBeDeleted.isEmpty

  private def loadLog(logDir: File,
                      recoveryPoints: Map[TopicPartition, Long],
                      logStartOffsets: Map[TopicPartition, Long]): Log = {
    // 解析日志目录的TopicPartition名称
    val topicPartition = Log.parseTopicPartitionName(logDir)
    // 获取 Topic 的配置，如果存在则返回对应的配置，否则返回默认配置
    val config = topicConfigs.getOrElse(topicPartition.topic, currentDefaultConfig)
    // 获取 Topic 的恢复点，如果存在则返回对应的恢复点，否则返回0
    val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)
    // 获取Topic的日志起始偏移量，如果存在则返回对应的日志起始偏移量，否则返回0
    val logStartOffset = logStartOffsets.getOrElse(topicPartition, 0L)

    // 初始化 Log 对象
    val log = Log(
      dir = logDir, // 日志目录
      config = config,  // 配置
      logStartOffset = logStartOffset, // 日志起始偏移量
      recoveryPoint = logRecoveryPoint, // 恢复点
      maxProducerIdExpirationMs = maxPidExpirationMs, // 最大生产者ID过期时间
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs, // 生产者ID过期检查时间间隔
      scheduler = scheduler, // 调度器
      time = time,
      brokerTopicStats = brokerTopicStats,  // BrokerTopic统计信息
      logDirFailureChannel = logDirFailureChannel) // 日志目录失败通道

    // 如果以'-delete'为后缀，加入addLogToBeDeleted队列等待删除的定时任务
    if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
      addLogToBeDeleted(log)
    } else {
      val previous = {
        if (log.isFuture)
          this.futureLogs.put(topicPartition, log)
        else
          this.currentLogs.put(topicPartition, log)
      }
      if (previous != null) {
        if (log.isFuture)
          throw new IllegalStateException(s"Duplicate log directories found: ${log.dir.getAbsolutePath}, ${previous.dir.getAbsolutePath}")
        else
          throw new IllegalStateException(s"Duplicate log directories for $topicPartition are found in both ${log.dir.getAbsolutePath} " +
            s"and ${previous.dir.getAbsolutePath}. It is likely because log directory failure happened while broker was " +
            s"replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
            s"for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.")
      }
    }

    // 返回日志对象
    log
  }

  /**
   * Recover and load all logs in the given data directories
   */
  private def loadLogs(): Unit = {
    info(s"Loading logs from log dirs $liveLogDirs")
    val startMs = time.hiResClockMs()
    // 创建线程池数组。
    val threadPools = ArrayBuffer.empty[ExecutorService]
    val offlineDirs = mutable.Set.empty[(String, IOException)]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]
    var numTotalLogs = 0

    // 遍历每个数据目录
    for (dir <- liveLogDirs) {
      val logDirAbsolutePath = dir.getAbsolutePath
      try {
        // 对每个日志目录创建 numRecoveryThreadsPerDataDir 个线程组成的线程池，并加入threadPools 线程池组中，
        // 这里 numRecoveryThreadsPerDataDir 默认为 1。
        val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
        threadPools.append(pool)

        // 检测上次的节点关闭是否是正常关闭。
        val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)
        // 检查 .kafka_cleanShutdown 文件是否存在，存在则表示 kafka 正在做清理性的停机工作，此时跳过从本文件夹恢复日志
        if (cleanShutdownFile.exists) {
          info(s"Skipping recovery for all logs in $logDirAbsolutePath since clean shutdown file was found")
        } else {
          // log recovery itself is being performed by `Log` class during initialization
          info(s"Attempting recovery for all logs in $logDirAbsolutePath since no clean shutdown file was found")
          brokerState.newState(RecoveringFromUncleanShutdown)
        }

        var recoveryPoints = Map[TopicPartition, Long]()
        try {
          // 加载每个日志目录的 recoveryPoints。
          /*
           * 从检查点文件读取 topic 对应的恢复点 offset 信息
           * 其文件为 recovery-point-offset-checkpoint
           * checkpoint file format:
           * line1 : version
           * line2 : expectedSize
           * nLines: (tp, offset)
           */
          recoveryPoints = this.recoveryPointCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading recovery-point-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting the recovery checkpoint to 0", e)
        }

        /*
         * 加载每个日志目录的 logStartOffsetPoints。
         * 从检查点文件读取 topic 对应的 startOffset 信息
         * 其文件为 log-start-offset-checkpoint
         * checkpoint file 格式:
         * line1 : version
         * line2 : expectedSize
         * nLines: (tp, startOffset)
         */
        var logStartOffsets = Map[TopicPartition, Long]()
        try {
          logStartOffsets = this.logStartOffsetCheckpoints(dir).read()
        } catch {
          case e: Exception =>
            warn(s"Error occurred while reading log-start-offset-checkpoint file of directory " +
              s"$logDirAbsolutePath, resetting to the base offset of the first segment", e)
        }

        // 要加载的日志文件夹。
        val logsToLoad = Option(dir.listFiles).getOrElse(Array.empty).filter(_.isDirectory)
        val numLogsLoaded = new AtomicInteger(0)
        numTotalLogs += logsToLoad.length

        // 为每个日志子目录生成一个线程池的具体 job 任务，并提交到线程池中，其中每个 job 的主要任务是通过 loadLog() 方法加载日志
        val jobsForDir = logsToLoad.map { logDir =>
          val runnable: Runnable = () => {
            try {
              debug(s"Loading log $logDir")

              val logLoadStartMs = time.hiResClockMs()
              // 每个日志子目录都生成一个 job 任务去加载 log，创建 Log 对象。根据读取的recoveryPoints，logStartOffsets 进行加载，
              // 并把 log 对象加入到 logs 集合中。
              val log = loadLog(logDir, recoveryPoints, logStartOffsets)
              val logLoadDurationMs = time.hiResClockMs() - logLoadStartMs
              val currentNumLoaded = numLogsLoaded.incrementAndGet()

              info(s"Completed load of $log with ${log.numberOfSegments} segments in ${logLoadDurationMs}ms " +
                s"($currentNumLoaded/${logsToLoad.length} loaded in $logDirAbsolutePath)")
            } catch {
              case e: IOException =>
                offlineDirs.add((logDirAbsolutePath, e))
                error(s"Error while loading log dir $logDirAbsolutePath", e)
            }
          }
          runnable
        }

        // 把 job 任务交给线程池进行处理,jobsForDir 是 List[Runnable] 类型
        jobs(cleanShutdownFile) = jobsForDir.map(pool.submit)
      } catch {
        case e: IOException =>
          offlineDirs.add((logDirAbsolutePath, e))
          error(s"Error while loading log dir $logDirAbsolutePath", e)
      }
    }

    try {
      // 阻塞等待上面提交的日志加载任务执行完成，即等待所有 log 目录下 topic 分区对应的目录文件加载完成，删除对应的cleanShutdownFile
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        try {
          // 删除对应的 .kafka_cleanshutdown 文件
          cleanShutdownFile.delete()
        } catch {
          case e: IOException =>
            offlineDirs.add((cleanShutdownFile.getParent, e))
            error(s"Error while deleting the clean shutdown file $cleanShutdownFile", e)
        }
      }

      offlineDirs.foreach { case (dir, e) =>
        logDirFailureChannel.maybeAddOfflineLogDir(dir, s"Error while deleting the clean shutdown file in dir $dir", e)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during logs loading: ${e.getCause}")
        throw e.getCause
    } finally {
      // 遍历关闭线程池
      threadPools.foreach(_.shutdown())
    }

    info(s"Loaded $numTotalLogs logs in ${time.hiResClockMs() - startMs}ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup(): Unit = {
    /* Schedule the cleanup task to delete old logs */
    // 后台线程池不为空的情况下进行日志管理后台工作的启动，这里会启动五个定时调度的任务。
    if (scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      // 启动 kafka-log-retention 周期性任务，遍历所有的log，对过期或过大的日志文件执行清理工作。
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs _, // 定时执行的方法
                         delay = InitialTaskDelayMs, // 启动之后30s开始定时调度
                         period = retentionCheckMs, // log.retention.check.interval.ms 默认为5分钟。
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      // 启动 kafka-log-flusher 周期性任务，对日志文件执行刷盘操作，定时把内存中的数据刷到磁盘中。
      scheduler.schedule("kafka-log-flusher",
                         flushDirtyLogs _, // 定时执行的方法
                         delay = InitialTaskDelayMs,  // 启动之后30s开始定时调度
                         period = flushCheckMs, // log.flush.scheduler.interal.ms 默认值为Long.MaxValue。也就是说默认不刷盘，操作系统自己刷盘。
                         TimeUnit.MILLISECONDS)
      // 启动 kafka-recovery-point-checkpoint 周期性任务，更新 kafka-recovery-point-checkpoint 文件。
      // 向路径中写入当前的恢复点，避免在重启的时候重新恢复全部数据。
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointLogRecoveryOffsets _, // 定时执行的方法
                         delay = InitialTaskDelayMs, // 启动之后30s开始定时调度
                         period = flushRecoveryOffsetCheckpointMs, // log.flush.offset.checkpoint.interval.ms 默认为1分钟。
                         TimeUnit.MILLISECONDS)
      // 启动 kafka-log-start-offset-checkpoint 周期性任务，更新 kafka-log-start-offset-checkpoint 文件。
      // 向日志目录写入当前存储日志中的 start offset，避免读到已经被删除的日志。
      scheduler.schedule("kafka-log-start-offset-checkpoint",
                         checkpointLogStartOffsets _, // 定时执行的方法
                         delay = InitialTaskDelayMs, // 启动之后30s开始定时调度
                         period = flushStartOffsetCheckpointMs, // log.flush.start.offset.checkpoint.interval.ms 默认值为1分钟
                         TimeUnit.MILLISECONDS)
      // 启动 kafka-delete-logs 周期性任务，清理已经被标记为删除的日志。
      scheduler.schedule("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                         deleteLogs _, // 定时执行的方法
                         delay = InitialTaskDelayMs, // 启动之后30s开始定时调度
                         unit = TimeUnit.MILLISECONDS)
    }
    // 日志清理压缩线程启动
    if (cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown(): Unit = {
    info("Shutting down.")

    removeMetric("OfflineLogDirectoryCount")
    for (dir <- logDirs) {
      removeMetric("LogDirectoryOffline", Map("logDirectory" -> dir.getAbsolutePath))
    }

    val threadPools = ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown(), this)
    }

    val localLogsByDir = logsByDir

    // close logs in each dir
    for (dir <- liveLogDirs) {
      debug(s"Flushing and closing logs at $dir")

      val pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir)
      threadPools.append(pool)

      val logs = logsInDir(localLogsByDir, dir).values

      val jobsForDir = logs.map { log =>
        val runnable: Runnable = () => {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
        runnable
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }

    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        val logs = logsInDir(localLogsByDir, dir)

        // update the last flush point
        debug(s"Updating recovery points at $dir")
        checkpointRecoveryOffsetsAndCleanSnapshotsInDir(dir, logs, logs.values.toSeq)

        debug(s"Updating log start offsets at $dir")
        checkpointLogStartOffsetsInDir(dir, logs)

        // mark that the shutdown was clean by creating marker file
        debug(s"Writing clean shutdown marker at $dir")
        CoreUtils.swallow(Files.createFile(new File(dir, Log.CleanShutdownFile).toPath), this)
      }
    } catch {
      case e: ExecutionException =>
        error(s"There was an error in one of the threads during LogManager shutdown: ${e.getCause}")
        throw e.getCause
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }

  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long], isFuture: Boolean): Unit = {
    val affectedLogs = ArrayBuffer.empty[Log]
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = {
        if (isFuture)
          futureLogs.get(topicPartition)
        else
          currentLogs.get(topicPartition)
      }
      // If the log does not exist, skip it
      if (log != null) {
        // May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner = truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && !isFuture)
          abortAndPauseCleaning(topicPartition)
        try {
          if (log.truncateTo(truncateOffset))
            affectedLogs += log
          if (needToStopCleaner && !isFuture)
            maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
        } finally {
          if (needToStopCleaner && !isFuture)
            resumeCleaning(topicPartition)
        }
      }
    }

    for ((dir, logs) <- affectedLogs.groupBy(_.parentDirFile)) {
      checkpointRecoveryOffsetsAndCleanSnapshotsInDir(dir, logs)
    }
  }

  /**
   * Delete all data in a partition and start the log at the new offset
   *
   * @param topicPartition The partition whose log needs to be truncated
   * @param newOffset The new offset to start the log with
   * @param isFuture True iff the truncation should be performed on the future log of the specified partition
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long, isFuture: Boolean): Unit = {
    val log = {
      if (isFuture)
        futureLogs.get(topicPartition)
      else
        currentLogs.get(topicPartition)
    }
    // If the log does not exist, skip it
    if (log != null) {
      // Abort and pause the cleaning of the log, and resume after truncation is done.
      if (!isFuture)
        abortAndPauseCleaning(topicPartition)
      try {
        log.truncateFullyAndStartAt(newOffset)
        if (!isFuture)
          maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition)
      } finally {
        if (!isFuture)
          resumeCleaning(topicPartition)
      }
      checkpointRecoveryOffsetsAndCleanSnapshotsInDir(log.parentDirFile, Seq(log))
    }
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory
   * to avoid recovering the whole log on startup.
   */
  def checkpointLogRecoveryOffsets(): Unit = {
    // 获取所有主题分区的日志文件，将它们按照目录进行分组，存储在变量 logsByDirCached 中。
    val logsByDirCached = logsByDir
    // 循环每个主题分区日志存储目录
    liveLogDirs.foreach { logDir =>
      // 获取当前目录下的所有主题分区日志文件。
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      // 检查点恢复偏移并清理快照文件。
      checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsToCheckpoint, logsToCheckpoint.values.toSeq)
    }
  }

  /**
   * Write out the current log start offset for all logs to a text file in the log directory
   * to avoid exposing data that have been deleted by DeleteRecordsRequest
   */
  // 在指定目录中检查点各个主题分区的起始偏移量。
  def checkpointLogStartOffsets(): Unit = {
    // 它缓存所有主题分区的日志文件，将它们按照目录进行分组，存储在变量 logsByDirCached 中
    val logsByDirCached = logsByDir
    liveLogDirs.foreach { logDir =>
      // 它对于每个主题分区日志存储目录，调用 checkpointLogStartOffsetsInDir 方法，进行检查点操作
      checkpointLogStartOffsetsInDir(logDir, logsInDir(logsByDirCached, logDir))
    }
  }

  /**
   * Checkpoint recovery offsets for all the logs in logDir and clean the snapshots of all the
   * provided logs.
   *
   * @param logDir the directory in which the logs to be checkpointed are
   * @param logsToCleanSnapshot the logs whose snapshots will be cleaned
   */
  // Only for testing
  private[log] def checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir: File, logsToCleanSnapshot: Seq[Log]): Unit = {
    checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsInDir(logDir), logsToCleanSnapshot)
  }

  /**
   * Checkpoint recovery offsets for all the provided logs and clean the snapshots of all the
   * provided logs.
   *
   * @param logDir the directory in which the logs are
   * @param logsToCheckpoint the logs to be checkpointed
   * @param logsToCleanSnapshot the logs whose snapshots will be cleaned
   */
  // logDir 存储日志文件的目录
  // logsToCheckpoint 需要进行检查点恢复偏移的日志文件
  // logsToCleanSnapshot 需要清理快照文件的日志文件列表, toSeq 表示将日志文件从 Map 类型转换为序列类型
  private def checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, Log],
                                                              logsToCleanSnapshot: Seq[Log]): Unit = {
    try {
      // 查找指定目录中的恢复检查点文件，并对其中的恢复偏移量进行更新。
      recoveryPointCheckpoints.get(logDir).foreach { checkpoint =>
        // 构造一个 Map，将每个日志文件的主题分区和恢复偏移量配对。
        val recoveryOffsets = logsToCheckpoint.map { case (tp, log) => tp -> log.recoveryPoint }
        // 将 recoveryOffsets 写入恢复检查点文件
        checkpoint.write(recoveryOffsets)
      }
      // 循环遍历需要清理快照的日志文件列表，对其进行快照清理。每个日志文件会删除其恢复检查点后的快照文件。
      logsToCleanSnapshot.foreach(_.deleteSnapshotsAfterRecoveryPointCheckpoint())
    } catch {
      // 如果磁盘发生错误，则记录错误信息。
      case e: KafkaStorageException =>
        error(s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}")
      // 如果发生输入输出错误，则将该目录标记为离线，并且记录错误信息。
      case e: IOException =>
        logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath,
          s"Disk error while writing recovery offsets checkpoint in directory $logDir: ${e.getMessage}", e)
    }
  }

  /**
   * Checkpoint log start offsets for all the provided logs in the provided directory.
   *
   * @param logDir the directory in which logs are checkpointed
   * @param logsToCheckpoint the logs to be checkpointed
   */
  // 在指定目录中检查点各个主题分区的起始偏移量，将它们写入检查点文件中。
  private def checkpointLogStartOffsetsInDir(logDir: File, logsToCheckpoint: Map[TopicPartition, Log]): Unit = {
    try {
      // 首先查找指定目录中的起始偏移量检查点文件
      logStartOffsetCheckpoints.get(logDir).foreach { checkpoint =>
        // 收集起始偏移量信息
        val logStartOffsets = logsToCheckpoint.collect {
          // 这里构造一个 Map，将每个主题分区和其起始偏移量配对。每个主题分区的起始偏移量为其日志文件的 logStartOffset 属性，
          // 但需要保证其不小于第一个日志段的基本偏移量
          case (tp, log) if log.logStartOffset > log.logSegments.head.baseOffset => tp -> log.logStartOffset
        }
        // 将 logStartOffsets 写入起始偏移量检查点文件
        checkpoint.write(logStartOffsets)
      }
    } catch {
      // 如果磁盘发生错误，则记录错误信息。
      case e: KafkaStorageException =>
        error(s"Disk error while writing log start offsets checkpoint in directory $logDir: ${e.getMessage}")
    }
  }

  // The logDir should be an absolute path
  def maybeUpdatePreferredLogDir(topicPartition: TopicPartition, logDir: String): Unit = {
    // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
    if (!getLog(topicPartition).exists(_.parentDir == logDir) &&
        !getLog(topicPartition, isFuture = true).exists(_.parentDir == logDir))
      preferredLogDirs.put(topicPartition, logDir)
  }

  /**
   * Abort and pause cleaning of the provided partition and log a message about it.
   */
  def abortAndPauseCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.abortAndPauseCleaning(topicPartition)
      info(s"The cleaning for partition $topicPartition is aborted and paused")
    }
  }

  /**
   * Resume cleaning of the provided partition and log a message about it.
   */
  private def resumeCleaning(topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.resumeCleaning(Seq(topicPartition))
      info(s"Cleaning for partition $topicPartition is resumed")
    }
  }

  /**
   * Truncate the cleaner's checkpoint to the based offset of the active segment of
   * the provided log.
   */
  private def maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log: Log, topicPartition: TopicPartition): Unit = {
    if (cleaner != null) {
      cleaner.maybeTruncateCheckpoint(log.parentDirFile, topicPartition, log.activeSegment.baseOffset)
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   *
   * @param topicPartition the partition of the log
   * @param isFuture True iff the future log of the specified partition should be returned
   */
  def getLog(topicPartition: TopicPartition, isFuture: Boolean = false): Option[Log] = {
    if (isFuture)
      Option(futureLogs.get(topicPartition))
    else
      Option(currentLogs.get(topicPartition))
  }

  /**
   * Method to indicate that logs are getting initialized for the partition passed in as argument.
   * This method should always be followed by [[kafka.log.LogManager#finishedInitializingLog]] to indicate that log
   * initialization is done.
   */
  def initializingLog(topicPartition: TopicPartition): Unit = {
    partitionsInitializing(topicPartition) = false
  }

  /**
   * Mark the partition configuration for all partitions that are getting initialized for topic
   * as dirty. That will result in reloading of configuration once initialization is done.
   */
  def topicConfigUpdated(topic: String): Unit = {
    partitionsInitializing.keys.filter(_.topic() == topic).foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Mark all in progress partitions having dirty configuration if broker configuration is updated.
   */
  def brokerConfigUpdated(): Unit = {
    partitionsInitializing.keys.foreach {
      topicPartition => partitionsInitializing.replace(topicPartition, false, true)
    }
  }

  /**
   * Method to indicate that the log initialization for the partition passed in as argument is
   * finished. This method should follow a call to [[kafka.log.LogManager#initializingLog]].
   *
   * It will retrieve the topic configs a second time if they were updated while the
   * relevant log was being loaded.
   */
  def finishedInitializingLog(topicPartition: TopicPartition,
                              maybeLog: Option[Log],
                              fetchLogConfig: () => LogConfig): Unit = {
    val removedValue = partitionsInitializing.remove(topicPartition)
    if (removedValue.contains(true))
      maybeLog.foreach(_.updateConfig(fetchLogConfig()))
  }

  /**
   * If the log already exists, just return a copy of the existing log
   * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
   * Otherwise throw KafkaStorageException
   *
   * @param topicPartition The partition whose log needs to be returned or created
   * @param loadConfig A function to retrieve the log config, this is only called if the log is created
   * @param isNew Whether the replica should have existed on the broker or not
   * @param isFuture True if the future log of the specified partition should be returned or created
   * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
   */
  // 用于获取或创建指定主题分区的日志对象
  def getOrCreateLog(topicPartition: TopicPartition, loadConfig: () => LogConfig, isNew: Boolean = false, isFuture: Boolean = false): Log = {
    // 加锁，确保创建和删除日志的安全性
    logCreationOrDeletionLock synchronized {
      // 获取指定主题分区的日志，如果不存在则进行创建
      getLog(topicPartition, isFuture).getOrElse {
        // create the log if it has not already been created in another thread
        // 如果不是新建日志且存在离线日志目录，则抛出异常
        if (!isNew && offlineLogDirs.nonEmpty)
          throw new KafkaStorageException(s"Can not create log for $topicPartition because log directories ${offlineLogDirs.mkString(",")} are offline")

        // 获取日志目录
        val logDirs: List[File] = {
          val preferredLogDir = preferredLogDirs.get(topicPartition)

          // 如果是 Future，则需要指定首选日志目录
          if (isFuture) {
            if (preferredLogDir == null)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition without having a preferred log directory")
            else if (getLog(topicPartition).get.parentDir == preferredLogDir)
              throw new IllegalStateException(s"Can not create the future log for $topicPartition in the current log directory of this partition")
          }

          // 如果存在首选日志目录，则使用首选日志目录创建日志目录列表
          if (preferredLogDir != null)
            List(new File(preferredLogDir))
          else
          // 否则使用下一个可用的日志目录
            nextLogDirs()
        }

        // 根据是否为 Future 来获取不同日志目录名称
        val logDirName = {
          if (isFuture)
            Log.logFutureDirName(topicPartition)
          else
            Log.logDirName(topicPartition)
        }

        // 遍历日志目录列表，找到可以成功创建日志目录的路径，如果找不到则抛出异常
        val logDir = logDirs
          .iterator // to prevent actually mapping the whole list, lazy map
          .map(createLogDirectory(_, logDirName))
          .find(_.isSuccess)
          .getOrElse(Failure(new KafkaStorageException("No log directories available. Tried " + logDirs.map(_.getAbsolutePath).mkString(", "))))
          .get // If Failure, will throw

        // 获取日志配置信息
        val config = loadConfig()
        // 创建日志对象
        val log = Log(
          dir = logDir,
          config = config,
          logStartOffset = 0L,
          recoveryPoint = 0L,
          maxProducerIdExpirationMs = maxPidExpirationMs,
          producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
          scheduler = scheduler,
          time = time,
          brokerTopicStats = brokerTopicStats,
          logDirFailureChannel = logDirFailureChannel)

        // 如果是 Future，则将日志对象添加到 futureLogs 中，否则添加到currentLogs中
        if (isFuture)
          futureLogs.put(topicPartition, log)
        else
          currentLogs.put(topicPartition, log)

        info(s"Created log for partition $topicPartition in $logDir with properties " + s"{${config.originals.asScala.mkString(", ")}}.")
        // Remove the preferred log dir since it has already been satisfied
        preferredLogDirs.remove(topicPartition)

        log
      }
    }
  }

  private[log] def createLogDirectory(logDir: File, logDirName: String): Try[File] = {
    val logDirPath = logDir.getAbsolutePath
    if (isLogDirOnline(logDirPath)) {
      val dir = new File(logDirPath, logDirName)
      try {
        Files.createDirectories(dir.toPath)
        Success(dir)
      } catch {
        case e: IOException =>
          val msg = s"Error while creating log for $logDirName in dir $logDirPath"
          logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, e)
          warn(msg, e)
          Failure(new KafkaStorageException(msg, e))
      }
    } else {
      Failure(new KafkaStorageException(s"Can not create log $logDirName because log directory $logDirPath is offline"))
    }
  }

  /**
   *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
   *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
   *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
   *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
   *  `deleteLogs` will be executed after `currentDefaultConfig.fileDeleteDelayMs`.
   */
  // 异步删除已被标记为过时的日志文件
  private def deleteLogs(): Unit = {
    var nextDelayMs = 0L
    try {
      // 计算即将删除的下一个日志文件距离当前时间的时间差，以确定下一次删除的时间间隔。
      // 如果有日志文件需要被删除，则返回时间差，否则返回默认值 fileDeleteDelayMs。
      def nextDeleteDelayMs: Long = {
        if (!logsToBeDeleted.isEmpty) {
          // 队列中取出
          val (_, scheduleTimeMs) = logsToBeDeleted.peek()
          scheduleTimeMs + currentDefaultConfig.fileDeleteDelayMs - time.milliseconds()
        } else
          currentDefaultConfig.fileDeleteDelayMs
      }

      // 2、用于删除已被标记为过时的日志文件。在每次循环中，计算下一次删除日志文件的时间间隔 nextDelayMs，并且如果 nextDelayMs 大于零，
      // 则方法会休眠相应的时间。然后从 logsToBeDeleted 队列中取出第一个即将删除的日志文件，并将其从队列中删除。
      // 随后删除该日志文件，并打印日志信息
      while ({nextDelayMs = nextDeleteDelayMs; nextDelayMs <= 0}) {
        // 从队列中取出
        val (removedLog, _) = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            // 删除
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: KafkaStorageException =>
              error(s"Exception while deleting $removedLog in dir ${removedLog.parentDir}.", e)
          }
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Exception in kafka-delete-logs thread.", e)
    } finally {
      try {
        // 重新创建一个定时任务
        // 使用定时调度器 scheduler 设置下一次删除操作的时间，并把方法自身作为回调函数进行调度。
        // 如果定时调度器抛出了异常，则将该方法标记为已停止，会导致该方法不再执行，并打印异常信息。
        scheduler.schedule("kafka-delete-logs",
          deleteLogs _,
          delay = nextDelayMs,
          unit = TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          if (scheduler.isStarted) {
            // No errors should occur unless scheduler has been shutdown
            error(s"Failed to schedule next delete in kafka-delete-logs thread", e)
          }
      }
    }
  }

  /**
    * Mark the partition directory in the source log directory for deletion and
    * rename the future log of this partition in the destination log directory to be the current log
    *
    * @param topicPartition TopicPartition that needs to be swapped
    */
  def replaceCurrentWithFutureLog(topicPartition: TopicPartition): Unit = {
    logCreationOrDeletionLock synchronized {
      val sourceLog = currentLogs.get(topicPartition)
      val destLog = futureLogs.get(topicPartition)

      info(s"Attempting to replace current log $sourceLog with $destLog for $topicPartition")
      if (sourceLog == null)
        throw new KafkaStorageException(s"The current replica for $topicPartition is offline")
      if (destLog == null)
        throw new KafkaStorageException(s"The future replica for $topicPartition is offline")

      destLog.renameDir(Log.logDirName(topicPartition))
      destLog.updateHighWatermark(sourceLog.highWatermark)

      // Now that future replica has been successfully renamed to be the current replica
      // Update the cached map and log cleaner as appropriate.
      futureLogs.remove(topicPartition)
      currentLogs.put(topicPartition, destLog)
      if (cleaner != null) {
        cleaner.alterCheckpointDir(topicPartition, sourceLog.parentDirFile, destLog.parentDirFile)
        resumeCleaning(topicPartition)
      }

      try {
        sourceLog.renameDir(Log.logDeleteDirName(topicPartition))
        // Now that replica in source log directory has been successfully renamed for deletion.
        // Close the log, update checkpoint files, and enqueue this log to be deleted.
        sourceLog.close()
        val logDir = sourceLog.parentDirFile
        val logsToCheckpoint = logsInDir(logDir)
        checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsToCheckpoint, ArrayBuffer.empty)
        checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        sourceLog.removeLogMetrics()
        addLogToBeDeleted(sourceLog)
      } catch {
        case e: KafkaStorageException =>
          // If sourceLog's log directory is offline, we need close its handlers here.
          // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
          sourceLog.closeHandlers()
          sourceLog.removeLogMetrics()
          throw e
      }

      info(s"The current replica is successfully replaced with the future replica for $topicPartition")
    }
  }

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
    * add it in the queue for deletion.
    *
    * @param topicPartition TopicPartition that needs to be deleted
    * @param isFuture True iff the future log of the specified partition should be deleted
    * @param checkpoint True if checkpoints must be written
    * @return the removed log
    */
  def asyncDelete(topicPartition: TopicPartition,
                  isFuture: Boolean = false,
                  checkpoint: Boolean = true): Option[Log] = {
    // 使用同步锁确保线程安全
    val removedLog: Option[Log] = logCreationOrDeletionLock synchronized {
      // 从 currentLogs 或者 futureLogs 中移除指定的 topicPartition 的 Log，并返回被移除的 Log
      removeLogAndMetrics(if (isFuture) futureLogs else currentLogs, topicPartition)
    }
    // 根据 removeLogAndMetrics 方法返回的结果 removedLog  进行处理
    removedLog match {
      case Some(removedLog) =>
        // We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
        // 我们需要等到要删除的日志上没有更多的清理任务，然后才能真正删除它。
        if (cleaner != null && !isFuture) {
          // 终止正在进行的 Log 清理任务
          cleaner.abortCleaning(topicPartition)
          if (checkpoint) {
            // 更新 Checkpoint 文件以进行删除操作
            cleaner.updateCheckpoints(removedLog.parentDirFile, partitionToRemove = Option(topicPartition))
          }
        }
        // 将被删除的 Log 的目录重命名为日志删除目录，命名规则 topic-uuid-delete
        removedLog.renameDir(Log.logDeleteDirName(topicPartition))
        if (checkpoint) {
          val logDir = removedLog.parentDirFile
          // 获取目录下的 Log 文件
          val logsToCheckpoint = logsInDir(logDir)
          // 对 Log 文件进行 Checkpoint
          checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsToCheckpoint, ArrayBuffer.empty)
          checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
        }
        // 将被删除的 Log 添加到待删除列队列中，稍后会被异步删除
        addLogToBeDeleted(removedLog)
        info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")

      case None =>
        if (offlineLogDirs.nonEmpty) {
          // 如果无法删除 Log，可能是因为 Log 所在的目录处于离线状态，抛出异常提示
          throw new KafkaStorageException(s"Failed to delete log for ${if (isFuture) "future" else ""} $topicPartition because it may be in one of the offline directories ${offlineLogDirs.mkString(",")}")
        }
    }

    removedLog
  }

  /**
   * Rename the directories of the given topic-partitions and add them in the queue for
   * deletion. Checkpoints are updated once all the directories have been renamed.
   *
   * @param topicPartitions The set of topic-partitions to delete asynchronously
   * @param errorHandler The error handler that will be called when a exception for a particular
   *                     topic-partition is raised
   */
  def asyncDelete(topicPartitions: Set[TopicPartition],
                  errorHandler: (TopicPartition, Throwable) => Unit): Unit = {
    val logDirs = mutable.Set.empty[File]

    topicPartitions.foreach { topicPartition =>
      try {
        getLog(topicPartition).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, checkpoint = false)
        }
        getLog(topicPartition, isFuture = true).foreach { log =>
          logDirs += log.parentDirFile
          asyncDelete(topicPartition, isFuture = true, checkpoint = false)
        }
      } catch {
        case e: Throwable => errorHandler(topicPartition, e)
      }
    }

    val logsByDirCached = logsByDir
    logDirs.foreach { logDir =>
      if (cleaner != null) cleaner.updateCheckpoints(logDir)
      val logsToCheckpoint = logsInDir(logsByDirCached, logDir)
      checkpointRecoveryOffsetsAndCleanSnapshotsInDir(logDir, logsToCheckpoint, ArrayBuffer.empty)
      checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint)
    }
  }

  /**
   * Provides the full ordered list of suggested directories for the next partition.
   * Currently this is done by calculating the number of partitions in each directory and then sorting the
   * data directories by fewest partitions.
   */
  private def nextLogDirs(): List[File] = {
    if(_liveLogDirs.size == 1) {
      List(_liveLogDirs.peek())
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.parentDir).map { case (parent, logs) => parent -> logs.size }
      val zeros = _liveLogDirs.asScala.map(dir => (dir.getPath, 0)).toMap
      val dirCounts = (zeros ++ logCounts).toBuffer

      // choose the directory with the least logs in it
      dirCounts.sortBy(_._2).map {
        case (path: String, _: Int) => new File(path)
      }.toList
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   */
  // 日志清除任务
  def cleanupLogs(): Unit = {
    // 记录日志，表示开始清理操作
    debug("Beginning log cleanup...")
    // 定义变量 total，用于存储删除的日志总数。
    var total = 0
    // 变量 startMs，存储方法开始执行时的时间戳，用于记录操作所需时间
    val startMs = time.milliseconds

    // clean current logs.
    // 初始化变量 deletableLogs，它是删除操作的目标，没有日志记录状态更改的话先返回 currentLogs，否则调用清理功能，并从中筛选出非压缩日志。
    val deletableLogs = {
      if (cleaner != null) {
        // prevent cleaner from working on same partitions when changing cleanup policy
        // 1、先中断当前 topic-partition 下正在进行执行 cleaner 的线程
        cleaner.pauseCleaningForNonCompactedPartitions()
      } else {
        // 2、过滤掉 cleanup.policy 配置的不是 delete 的 log
        currentLogs.filter {
          case (_, log) => !log.config.compact
        }
      }
    }

    // 到这里了 deletableLogs 中 log 的 cleanup.policy 配置是 delete，开始进行删除
    try {
      // 遍历被筛选的删除日志文件列表，执行以下操作。
      deletableLogs.foreach {
        // 匹配主题和分区，并将其分配给变量 topicPartition 和 log。
        case (topicPartition, log) =>
          // 记录日志，表示开始清理日志。
          debug(s"Garbage collecting '${log.name}'")
          // 委托给 log.deleteOldSegments() 方法进行删除过期的 `logSegment`。
          total += log.deleteOldSegments()

          // 获取分区主题的日志段，并将其分配给变量 futureLog。
          val futureLog = futureLogs.get(topicPartition)
          if (futureLog != null) {
            // clean future logs
            debug(s"Garbage collecting future log '${futureLog.name}'")
            // 调用日志模块的方法 deleteOldSegments() 删除日志段。
            total += futureLog.deleteOldSegments()
          }
      }
    } finally {
      if (cleaner != null) {
        // 如果存在清理功能，则唤醒清理线程。
        // 如果在第 1 步中中断了 cleaner 线程，则在这里进行恢复。也就是说在 topic-partition 的同一时刻 只有一个 cleaner 对其进行清理
        cleaner.resumeCleaning(deletableLogs.map(_._1))
      }
    }

    debug(s"Log cleanup completed. $total files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs: Iterable[Log] = currentLogs.values ++ futureLogs.values

  def logsByTopic(topic: String): Seq[Log] = {
    (currentLogs.toList ++ futureLogs.toList).collect {
      case (topicPartition, log) if topicPartition.topic == topic => log
    }
  }

  /**
   * Map of log dir to logs by topic and partitions in that dir
   */
  private def logsByDir: Map[String, Map[TopicPartition, Log]] = {
    // This code is called often by checkpoint processes and is written in a way that reduces
    // allocations and CPU with many topic partitions.
    // When changing this code please measure the changes with org.apache.kafka.jmh.server.CheckpointBench
    val byDir = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Log]]()
    def addToDir(tp: TopicPartition, log: Log): Unit = {
      byDir.getOrElseUpdate(log.parentDir, new mutable.AnyRefMap[TopicPartition, Log]()).put(tp, log)
    }
    currentLogs.foreachEntry(addToDir)
    futureLogs.foreachEntry(addToDir)
    byDir
  }

  private def logsInDir(dir: File): Map[TopicPartition, Log] = {
    logsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  private def logsInDir(cachedLogsByDir: Map[String, Map[TopicPartition, Log]],
                        dir: File): Map[TopicPartition, Log] = {
    cachedLogsByDir.getOrElse(dir.getAbsolutePath, Map.empty)
  }

  // logDir should be an absolute path
  def isLogDirOnline(logDir: String): Boolean = {
    // The logDir should be an absolute path
    if (!logDirs.exists(_.getAbsolutePath == logDir))
      throw new LogDirNotFoundException(s"Log dir $logDir is not found in the config.")

    _liveLogDirs.contains(new File(logDir))
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs(): Unit = {
    debug("Checking for dirty logs to flush...")

    // 遍历 currentLogs 和  futureLogs 集合
    for ((topicPartition, log) <- currentLogs.toList ++ futureLogs.toList) {
      try {
        // 计算上一次 flush 的时间与当前时间差值
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug(s"Checking if flush is needed on ${topicPartition.topic} flush interval ${log.config.flushMs}" +
              s" last flushed ${log.lastFlushTime} time since last flush: $timeSinceLastFlush")
        // 如果满足 flush.ms 配置的时间，则调用 flush 方法刷新到磁盘上
        // 会把 [recoverPoint ~ LEO] 之间的消息数据刷新到磁盘上，并修改 recoverPoint 值
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush()
      } catch {
        case e: Throwable =>
          error(s"Error flushing topic ${topicPartition.topic}", e)
      }
    }
  }

  private def removeLogAndMetrics(logs: Pool[TopicPartition, Log], tp: TopicPartition): Option[Log] = {
    val removedLog = logs.remove(tp)
    if (removedLog != null) {
      removedLog.removeLogMetrics()
      Some(removedLog)
    } else {
      None
    }
  }
}

object LogManager {

  // 检查点表示日志已经刷新到磁盘的位置，主要是用于数据恢复
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LogStartOffsetCheckpointFile = "log-start-offset-checkpoint"
  val ProducerIdExpirationCheckIntervalMs = 10 * 60 * 1000

  // 在 scala 中 该方法相当于构造函数，用于创建 LogManager 实例
  def apply(config: KafkaConfig,
            initialOfflineDirs: Seq[String],
            zkClient: KafkaZkClient,
            brokerState: BrokerState,
            kafkaScheduler: KafkaScheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel): LogManager = {
    // 将主配置转换为日志配置
    val defaultProps = KafkaServer.copyKafkaConfigToLog(config)
    // 验证默认配置中的值，以使用正确的类型和最小/最大值替换设置的非法值
    LogConfig.validateValues(defaultProps)
    // 加载日志配置
    val defaultLogConfig = LogConfig(defaultProps)

    // read the log configurations from zookeeper
    // 从 ZooKeeper 中读取所有日志的配置，包括主题和分区的配置信息，并返回失败列表。
    // 这个方法提供了直接到读取到的日志配置数据集的连续迭代，以及它们读取失败的各种错误。
    val (topicConfigs, failed) = zkClient.getLogConfigs(
      zkClient.getAllTopicsInCluster(),
      defaultProps
    )
    if (!failed.isEmpty) throw failed.head._2

    // 通过 LogCleaner 对象构建 LogCleanerConfig 对象
    val cleanerConfig = LogCleaner.cleanerConfig(config)

    // 实例化 LogManager 对象
    new LogManager(logDirs = config.logDirs.map(new File(_).getAbsoluteFile),
      initialOfflineDirs = initialOfflineDirs.map(new File(_).getAbsoluteFile),
      topicConfigs = topicConfigs,
      initialDefaultConfig = defaultLogConfig,
      cleanerConfig = cleanerConfig,
      recoveryThreadsPerDataDir = config.numRecoveryThreadsPerDataDir,
      flushCheckMs = config.logFlushSchedulerIntervalMs,
      flushRecoveryOffsetCheckpointMs = config.logFlushOffsetCheckpointIntervalMs,
      flushStartOffsetCheckpointMs = config.logFlushStartOffsetCheckpointIntervalMs,
      retentionCheckMs = config.logCleanupIntervalMs,
      maxPidExpirationMs = config.transactionalIdExpirationMs,
      scheduler = kafkaScheduler,
      brokerState = brokerState,
      brokerTopicStats = brokerTopicStats,
      logDirFailureChannel = logDirFailureChannel,
      time = time)
  }
}
