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
package kafka.cluster

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.{Optional, Properties}

import kafka.api.{ApiVersion, LeaderAndIsr}
import kafka.common.UnexpectedAppendOffsetException
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import kafka.zookeeper.ZooKeeperClientException
import org.apache.kafka.common.errors._
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record.{MemoryRecords, RecordBatch}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{IsolationLevel, TopicPartition}

import scala.collection.{Map, Seq}
import scala.jdk.CollectionConverters._

trait IsrChangeListener {
  def markExpand(): Unit
  def markShrink(): Unit
  def markFailed(): Unit
}

trait PartitionStateStore {
  def fetchTopicConfig(): Properties
  def shrinkIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int]
  def expandIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int]
}

class ZkPartitionStateStore(topicPartition: TopicPartition,
                            zkClient: KafkaZkClient) extends PartitionStateStore {

  override def fetchTopicConfig(): Properties = {
    val adminZkClient = new AdminZkClient(zkClient)
    adminZkClient.fetchEntityConfig(ConfigType.Topic, topicPartition.topic)
  }

  override def shrinkIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int] = {
    val newVersionOpt = updateIsr(controllerEpoch, leaderAndIsr)
    newVersionOpt
  }

  override def expandIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int] = {
    val newVersionOpt = updateIsr(controllerEpoch, leaderAndIsr)
    newVersionOpt
  }

  private def updateIsr(controllerEpoch: Int, leaderAndIsr: LeaderAndIsr): Option[Int] = {
    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, topicPartition,
      leaderAndIsr, controllerEpoch)

    if (updateSucceeded) {
      Some(newVersion)
    } else {
      None
    }
  }
}

class DelayedOperations(topicPartition: TopicPartition,
                        produce: DelayedOperationPurgatory[DelayedProduce],
                        fetch: DelayedOperationPurgatory[DelayedFetch],
                        deleteRecords: DelayedOperationPurgatory[DelayedDeleteRecords]) {

  def checkAndCompleteAll(): Unit = {
    val requestKey = TopicPartitionOperationKey(topicPartition)
    fetch.checkAndComplete(requestKey)
    produce.checkAndComplete(requestKey)
    deleteRecords.checkAndComplete(requestKey)
  }

  def numDelayedDelete: Int = deleteRecords.numDelayed
}

object Partition extends KafkaMetricsGroup {
  def apply(topicPartition: TopicPartition,
            time: Time,
            replicaManager: ReplicaManager): Partition = {

    val isrChangeListener = new IsrChangeListener {
      override def markExpand(): Unit = {
        replicaManager.recordIsrChange(topicPartition)
        replicaManager.isrExpandRate.mark()
      }

      override def markShrink(): Unit = {
        replicaManager.recordIsrChange(topicPartition)
        replicaManager.isrShrinkRate.mark()
      }

      override def markFailed(): Unit = replicaManager.failedIsrUpdatesRate.mark()
    }

    val zkIsrBackingStore = new ZkPartitionStateStore(
      topicPartition,
      replicaManager.zkClient)

    val delayedOperations = new DelayedOperations(
      topicPartition,
      replicaManager.delayedProducePurgatory,
      replicaManager.delayedFetchPurgatory,
      replicaManager.delayedDeleteRecordsPurgatory)

    new Partition(topicPartition,
      replicaLagTimeMaxMs = replicaManager.config.replicaLagTimeMaxMs,
      interBrokerProtocolVersion = replicaManager.config.interBrokerProtocolVersion,
      localBrokerId = replicaManager.config.brokerId,
      time = time,
      stateStore = zkIsrBackingStore,
      isrChangeListener = isrChangeListener,
      delayedOperations = delayedOperations,
      metadataCache = replicaManager.metadataCache,
      logManager = replicaManager.logManager,
      alterIsrManager = replicaManager.alterIsrManager)
  }

  def removeMetrics(topicPartition: TopicPartition): Unit = {
    val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString)
    removeMetric("UnderReplicated", tags)
    removeMetric("UnderMinIsr", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
    removeMetric("LastStableOffsetLag", tags)
    removeMetric("AtMinIsr", tags)
  }
}


sealed trait AssignmentState {
  def replicas: Seq[Int]
  def replicationFactor: Int = replicas.size
  def isAddingReplica(brokerId: Int): Boolean = false
}

case class OngoingReassignmentState(addingReplicas: Seq[Int],
                                    removingReplicas: Seq[Int],
                                    replicas: Seq[Int]) extends AssignmentState {

  override def replicationFactor: Int = replicas.diff(addingReplicas).size // keep the size of the original replicas
  override def isAddingReplica(replicaId: Int): Boolean = addingReplicas.contains(replicaId)
}

case class SimpleAssignmentState(replicas: Seq[Int]) extends AssignmentState



sealed trait IsrState {
  /**
   * Includes only the in-sync replicas which have been committed to ZK.
   */
  def isr: Set[Int]

  /**
   * This set may include un-committed ISR members following an expansion. This "effective" ISR is used for advancing
   * the high watermark as well as determining which replicas are required for acks=all produce requests.
   *
   * Only applicable as of IBP 2.7-IV2, for older versions this will return the committed ISR
   *
   */
  def maximalIsr: Set[Int]

  /**
   * Indicates if we have an AlterIsr request inflight.
   */
  def isInflight: Boolean
}

case class PendingExpandIsr(
  isr: Set[Int],
  newInSyncReplicaId: Int
) extends IsrState {
  val maximalIsr = isr + newInSyncReplicaId
  val isInflight = true

  override def toString: String = {
    s"PendingExpandIsr(isr=$isr" +
      s", newInSyncReplicaId=$newInSyncReplicaId" +
      ")"
  }
}

case class PendingShrinkIsr(
  isr: Set[Int],
  outOfSyncReplicaIds: Set[Int]
) extends IsrState  {
  val maximalIsr = isr
  val isInflight = true

  override def toString: String = {
    s"PendingShrinkIsr(isr=$isr" +
      s", outOfSyncReplicaIds=$outOfSyncReplicaIds" +
      ")"
  }
}

case class CommittedIsr(
  isr: Set[Int]
) extends IsrState {
  val maximalIsr = isr
  val isInflight = false

  override def toString: String = {
    s"CommittedIsr(isr=$isr" +
      ")"
  }
}


/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 *
 * Concurrency notes:
 * 1) Partition is thread-safe. Operations on partitions may be invoked concurrently from different
 *    request handler threads
 * 2) ISR updates are synchronized using a read-write lock. Read lock is used to check if an update
 *    is required to avoid acquiring write lock in the common case of replica fetch when no update
 *    is performed. ISR update condition is checked a second time under write lock before performing
 *    the update
 * 3) Various other operations like leader changes are processed while holding the ISR write lock.
 *    This can introduce delays in produce and replica fetch requests, but these operations are typically
 *    infrequent.
 * 4) HW updates are synchronized using ISR read lock. @Log lock is acquired during the update with
 *    locking order Partition lock -> Log lock.
 * 5) lock is used to prevent the follower replica from being updated while ReplicaAlterDirThread is
 *    executing maybeReplaceCurrentWithFutureReplica() to replace follower replica with the future replica.
 */
class Partition(val topicPartition: TopicPartition,
                val replicaLagTimeMaxMs: Long,
                interBrokerProtocolVersion: ApiVersion,
                localBrokerId: Int,
                time: Time,
                stateStore: PartitionStateStore,
                isrChangeListener: IsrChangeListener,
                delayedOperations: DelayedOperations,
                metadataCache: MetadataCache,
                logManager: LogManager,
                alterIsrManager: AlterIsrManager) extends Logging with KafkaMetricsGroup {

  def topic: String = topicPartition.topic
  def partitionId: Int = topicPartition.partition

  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)
  private val remoteReplicasMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock

  // lock to prevent the follower replica log update while checking if the log dir could be replaced with future log.
  private val futureLogLock = new Object()
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  // start offset for 'leaderEpoch' above (leader epoch of the current leader for this partition),
  // defined when this broker is leader for partition
  @volatile private var leaderEpochStartOffsetOpt: Option[Long] = None
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  @volatile private[cluster] var isrState: IsrState = CommittedIsr(Set.empty)
  @volatile var assignmentState: AssignmentState = SimpleAssignmentState(Seq.empty)

  private val useAlterIsr: Boolean = interBrokerProtocolVersion.isAlterIsrSupported

  // Logs belonging to this partition. Majority of time it will be only one log, but if log directory
  // is getting changed (as a result of ReplicaAlterLogDirs command), we may have two logs until copy
  // completes and a switch to new location is performed.
  // log and futureLog variables defined below are used to capture this
  @volatile var log: Option[Log] = None
  // If ReplicaAlterLogDir command is in progress, this is future location of the log
  @volatile var futureLog: Option[Log] = None

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  this.logIdent = s"[Partition $topicPartition broker=$localBrokerId] "

  private val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated", () => if (isUnderReplicated) 1 else 0, tags)
  newGauge("InSyncReplicasCount", () => if (isLeader) isrState.isr.size else 0, tags)
  newGauge("UnderMinIsr", () => if (isUnderMinIsr) 1 else 0, tags)
  newGauge("AtMinIsr", () => if (isAtMinIsr) 1 else 0, tags)
  newGauge("ReplicasCount", () => if (isLeader) assignmentState.replicationFactor else 0, tags)
  newGauge("LastStableOffsetLag", () => log.map(_.lastStableOffsetLag).getOrElse(0), tags)

  def isUnderReplicated: Boolean = isLeader && (assignmentState.replicationFactor - isrState.isr.size) > 0

  def isUnderMinIsr: Boolean = leaderLogIfLocal.exists { isrState.isr.size < _.config.minInSyncReplicas }

  def isAtMinIsr: Boolean = leaderLogIfLocal.exists { isrState.isr.size == _.config.minInSyncReplicas }

  def isReassigning: Boolean = assignmentState.isInstanceOf[OngoingReassignmentState]

  def isAddingLocalReplica: Boolean = assignmentState.isAddingReplica(localBrokerId)

  def isAddingReplica(replicaId: Int): Boolean = assignmentState.isAddingReplica(replicaId)

  def inSyncReplicaIds: Set[Int] = isrState.isr

  /**
    * Create the future replica if 1) the current replica is not in the given log directory and 2) the future replica
    * does not exist. This method assumes that the current replica has already been created.
    *
    * @param logDir log directory
    * @param highWatermarkCheckpoints Checkpoint to load initial high watermark from
    * @return true iff the future replica is created
    */
  def maybeCreateFutureReplica(logDir: String, highWatermarkCheckpoints: OffsetCheckpoints): Boolean = {
    // The writeLock is needed to make sure that while the caller checks the log directory of the
    // current replica and the existence of the future replica, no other thread can update the log directory of the
    // current replica or remove the future replica.
    inWriteLock(leaderIsrUpdateLock) {
      val currentLogDir = localLogOrException.parentDir
      if (currentLogDir == logDir) {
        info(s"Current log directory $currentLogDir is same as requested log dir $logDir. " +
          s"Skipping future replica creation.")
        false
      } else {
        futureLog match {
          case Some(partitionFutureLog) =>
            val futureLogDir = partitionFutureLog.parentDir
            if (futureLogDir != logDir)
              throw new IllegalStateException(s"The future log dir $futureLogDir of $topicPartition is " +
                s"different from the requested log dir $logDir")
            false
          case None =>
            createLogIfNotExists(isNew = false, isFutureReplica = true, highWatermarkCheckpoints)
            true
        }
      }
    }
  }

  def createLogIfNotExists(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints): Unit = {
    isFutureReplica match {
      case true if futureLog.isEmpty =>
        val log = createLog(isNew, isFutureReplica, offsetCheckpoints)
        this.futureLog = Option(log)
      case false if log.isEmpty =>
        val log = createLog(isNew, isFutureReplica, offsetCheckpoints)
        this.log = Option(log)
      case _ => trace(s"${if (isFutureReplica) "Future Log" else "Log"} already exists.")
    }
  }

  // Visible for testing
  private[cluster] def createLog(isNew: Boolean, isFutureReplica: Boolean, offsetCheckpoints: OffsetCheckpoints): Log = {
    def fetchLogConfig: LogConfig = {
      val props = stateStore.fetchTopicConfig()
      LogConfig.fromProps(logManager.currentDefaultConfig.originals, props)
    }

    def updateHighWatermark(log: Log) = {
      val checkpointHighWatermark = offsetCheckpoints.fetch(log.parentDir, topicPartition).getOrElse {
        info(s"No checkpointed highwatermark is found for partition $topicPartition")
        0L
      }
      val initialHighWatermark = log.updateHighWatermark(checkpointHighWatermark)
      info(s"Log loaded for partition $topicPartition with initial high watermark $initialHighWatermark")
    }

    logManager.initializingLog(topicPartition)
    var maybeLog: Option[Log] = None
    try {
      val log = logManager.getOrCreateLog(topicPartition, () => fetchLogConfig, isNew, isFutureReplica)
      maybeLog = Some(log)
      updateHighWatermark(log)
      log
    } finally {
      logManager.finishedInitializingLog(topicPartition, maybeLog, () => fetchLogConfig)
    }
  }

  def getReplica(replicaId: Int): Option[Replica] = Option(remoteReplicasMap.get(replicaId))

  private def getReplicaOrException(replicaId: Int): Replica = getReplica(replicaId).getOrElse{
    throw new NotLeaderOrFollowerException(s"Replica with id $replicaId is not available on broker $localBrokerId")
  }

  private def checkCurrentLeaderEpoch(remoteLeaderEpochOpt: Optional[Integer]): Errors = {
    if (!remoteLeaderEpochOpt.isPresent) {
      Errors.NONE
    } else {
      val remoteLeaderEpoch = remoteLeaderEpochOpt.get
      val localLeaderEpoch = leaderEpoch
      if (localLeaderEpoch > remoteLeaderEpoch)
        Errors.FENCED_LEADER_EPOCH
      else if (localLeaderEpoch < remoteLeaderEpoch)
        Errors.UNKNOWN_LEADER_EPOCH
      else
        Errors.NONE
    }
  }

  private def getLocalLog(currentLeaderEpoch: Optional[Integer],
                          requireLeader: Boolean): Either[Log, Errors] = {
    checkCurrentLeaderEpoch(currentLeaderEpoch) match {
      case Errors.NONE =>
        if (requireLeader && !isLeader) {
          Right(Errors.NOT_LEADER_OR_FOLLOWER)
        } else {
          log match {
            case Some(partitionLog) =>
              Left(partitionLog)
            case _ =>
              Right(Errors.NOT_LEADER_OR_FOLLOWER)
          }
        }
      case error =>
        Right(error)
    }
  }

  def localLogOrException: Log = log.getOrElse {
    throw new NotLeaderOrFollowerException(s"Log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def futureLocalLogOrException: Log = futureLog.getOrElse {
    throw new NotLeaderOrFollowerException(s"Future log for partition $topicPartition is not available " +
      s"on broker $localBrokerId")
  }

  def leaderLogIfLocal: Option[Log] = {
    log.filter(_ => isLeader)
  }

  /**
   * Returns true if this node is currently leader for the Partition.
   */
  def isLeader: Boolean = leaderReplicaIdOpt.contains(localBrokerId)

  private def localLogWithEpochOrException(currentLeaderEpoch: Optional[Integer],
                                           requireLeader: Boolean): Log = {
    getLocalLog(currentLeaderEpoch, requireLeader) match {
      case Left(localLog) => localLog
      case Right(error) =>
        throw error.exception(s"Failed to find ${if (requireLeader) "leader" else ""} log for " +
          s"partition $topicPartition with leader epoch $currentLeaderEpoch. The current leader " +
          s"is $leaderReplicaIdOpt and the current epoch $leaderEpoch")
    }
  }

  // Visible for testing -- Used by unit tests to set log for this partition
  def setLog(log: Log, isFutureLog: Boolean): Unit = {
    if (isFutureLog)
      futureLog = Some(log)
    else
      this.log = Some(log)
  }

  // remoteReplicas will be called in the hot path, and must be inexpensive
  def remoteReplicas: Iterable[Replica] =
    remoteReplicasMap.values

  def futureReplicaDirChanged(newDestinationDir: String): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      futureLog.exists(_.parentDir != newDestinationDir)
    }
  }

  def removeFutureLocalReplica(deleteFromLogDir: Boolean = true): Unit = {
    inWriteLock(leaderIsrUpdateLock) {
      futureLog = None
      if (deleteFromLogDir)
        logManager.asyncDelete(topicPartition, isFuture = true)
    }
  }

  // Return true if the future replica exists and it has caught up with the current replica for this partition
  // Only ReplicaAlterDirThread will call this method and ReplicaAlterDirThread should remove the partition
  // from its partitionStates if this method returns true
  def maybeReplaceCurrentWithFutureReplica(): Boolean = {
    // lock to prevent the log append by followers while checking if the log dir could be replaced with future log.
    futureLogLock.synchronized {
      val localReplicaLEO = localLogOrException.logEndOffset
      val futureReplicaLEO = futureLog.map(_.logEndOffset)
      if (futureReplicaLEO.contains(localReplicaLEO)) {
        // The write lock is needed to make sure that while ReplicaAlterDirThread checks the LEO of the
        // current replica, no other thread can update LEO of the current replica via log truncation or log append operation.
        inWriteLock(leaderIsrUpdateLock) {
          futureLog match {
            case Some(futurePartitionLog) =>
              if (log.exists(_.logEndOffset == futurePartitionLog.logEndOffset)) {
                logManager.replaceCurrentWithFutureLog(topicPartition)
                log = futureLog
                removeFutureLocalReplica(false)
                true
              } else false
            case None =>
              // Future replica is removed by a non-ReplicaAlterLogDirsThread before this method is called
              // In this case the partition should have been removed from state of the ReplicaAlterLogDirsThread
              // Return false so that ReplicaAlterLogDirsThread does not have to remove this partition from the
              // state again to avoid race condition
              false
          }
        }
      } else false
    }
  }

  /**
   * Delete the partition. Note that deleting the partition does not delete the underlying logs.
   * The logs are deleted by the ReplicaManager after having deleted the partition.
   */
  def delete(): Unit = {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      remoteReplicasMap.clear()
      assignmentState = SimpleAssignmentState(Seq.empty)
      log = None
      futureLog = None
      isrState = CommittedIsr(Set.empty)
      leaderReplicaIdOpt = None
      leaderEpochStartOffsetOpt = None
      Partition.removeMetrics(topicPartition)
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  def getZkVersion: Int = this.zkVersion

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  def makeLeader(partitionState: LeaderAndIsrPartitionState,
                 highWatermarkCheckpoints: OffsetCheckpoints): Boolean = {
    // 使用 leaderIsrUpdateLock 加锁，避免多线程同时操作
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      // 记录做出领导决策的控制器的 epoch，用于后续更新 isr 时在 ZooKeeper 路径中维护决策者控制器的 epoch
      controllerEpoch = partitionState.controllerEpoch

      // 获取 ISR 副本集合列表
      val isr = partitionState.isr.asScala.map(_.toInt).toSet
      // 将在 AR 副本集合列表的加进来
      val addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt)
      // 将不在 AR 副本集合列表的移除
      val removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)

      // 更新以及分配副本
      updateAssignmentAndIsr(
        assignment = partitionState.replicas.asScala.map(_.toInt),
        isr = isr,
        addingReplicas = addingReplicas,
        removingReplicas = removingReplicas
      )
      try {
        // 如果日志不存在，则创建日志
        createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints)
      } catch {
        // 如果创建日志时出现 ZooKeeper 客户端异常，则打印错误日志，并返回 false（表示创建失败）
        case e: ZooKeeperClientException =>
          stateChangeLogger.error(s"A ZooKeeper client exception has occurred and makeLeader will be skipping the " +
            s"state change for the partition $topicPartition with leader epoch: $leaderEpoch ", e)

          return false
      }

      // Leader 日志
      val leaderLog = localLogOrException
      // Leader LEO
      val leaderEpochStartOffset = leaderLog.logEndOffset
      stateChangeLogger.info(s"Leader $topicPartition starts at leader epoch ${partitionState.leaderEpoch} from " +
        s"offset $leaderEpochStartOffset with high watermark ${leaderLog.highWatermark} " +
        s"ISR ${isr.mkString("[", ",", "]")} addingReplicas ${addingReplicas.mkString("[", ",", "]")} " +
        s"removingReplicas ${removingReplicas.mkString("[", ",", "]")}. Previous leader epoch was $leaderEpoch.")

      //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
      // 缓存 Leader Epoch，只有当有日志目录时才持久化
      leaderEpoch = partitionState.leaderEpoch
      leaderEpochStartOffsetOpt = Some(leaderEpochStartOffset)
      zkVersion = partitionState.zkVersion

      // Clear any pending AlterIsr requests and check replica state
      // 清除任何挂起的 AlterIsr 请求，并检查副本状态
      alterIsrManager.clearPending(topicPartition)

      // In the case of successive leader elections in a short time period, a follower may have
      // entries in its log from a later epoch than any entry in the new leader's log. In order
      // to ensure that these followers can truncate to the right offset, we must cache the new
      // leader epoch and the start offset since it should be larger than any epoch that a follower
      // would try to query.
      // 在短时间内连续进行多次领导选举时，一个 follower 可能有比新 leader 的日志更晚的 epoch 的记录。
      // 为了确保这些 follower 可以截断到正确的偏移量，我们必须缓存新的 leader epoch 和起始偏移量，
      // 因为它应该比任何一个 follower 试图查询的 epoch 大。
      leaderLog.maybeAssignEpochStartOffset(leaderEpoch, leaderEpochStartOffset)

      // 它代表该 Broker 成为分区的新 Leader 副本（分区原 Leader 副本不是当前分区）
      val isNewLeader = !isLeader
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      // 初始化副本的 lastCaughtUpTime、lastFetchTimeMs 和 lastFetchLeaderLogEndOffset
      remoteReplicas.foreach { replica =>
        val lastCaughtUpTimeMs = if (isrState.isr.contains(replica.brokerId)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(leaderEpochStartOffset, curTimeMs, lastCaughtUpTimeMs)
      }

      // 判断是否是新的 Leader
      if (isNewLeader) {
        // mark local replica as the leader after converting hw
        // 将本地副本标记为 Leader，并重置远程副本的日志结束偏移量
        leaderReplicaIdOpt = Some(localBrokerId)
        // reset log end offset for remote replicas
        // 重置远端 Follower 副本的 LEO
        remoteReplicas.foreach { replica =>
          replica.updateFetchState(
            followerFetchOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata,
            followerStartOffset = Log.UnknownOffset,
            followerFetchTimeMs = 0L,
            leaderEndOffset = Log.UnknownOffset)
        }
      }
      // we may need to increment high watermark since ISR could be down to 1
      // 如果满足更新 ISR 的条件，就更新 HW 信息。
      (maybeIncrementLeaderHW(leaderLog), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    //  一些延迟操作可能在高水位标记改变后被解除阻塞
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    // 返回新 Leader
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change and the new epoch is equal or one
   *  greater (that is, no updates have been missed), return false to indicate to the
   * replica manager that state is already correct and the become-follower steps can be skipped
   */
  def makeFollower(partitionState: LeaderAndIsrPartitionState,
                   highWatermarkCheckpoints: OffsetCheckpoints): Boolean = {
    // 使用 leaderIsrUpdateLock 加锁，避免多线程同时操作
    inWriteLock(leaderIsrUpdateLock) {
      // 分区 Leader 所在 Broker id
      val newLeaderBrokerId = partitionState.leader
      // 旧 LeaderEpoch
      val oldLeaderEpoch = leaderEpoch
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      // 记录控制器作出领导权决策的 epoch。在更新 isr 时保留决策者的 epoch 值，有助于维护 zookeeper 路径中的正确值。
      controllerEpoch = partitionState.controllerEpoch

      // 更新以及分配副本
      updateAssignmentAndIsr(
        assignment = partitionState.replicas.asScala.iterator.map(_.toInt).toSeq,
        isr = Set.empty[Int],
        addingReplicas = partitionState.addingReplicas.asScala.map(_.toInt),
        removingReplicas = partitionState.removingReplicas.asScala.map(_.toInt)
      )
      try {
        // 如果日志不存在，则创建日志
        createLogIfNotExists(partitionState.isNew, isFutureReplica = false, highWatermarkCheckpoints)
      } catch {
        // 如果创建日志时出现 ZooKeeper 客户端异常，则打印错误日志，并返回 false（表示创建失败）
        case e: ZooKeeperClientException =>
          stateChangeLogger.error(s"A ZooKeeper client exception has occurred. makeFollower will be skipping the " +
            s"state change for the partition $topicPartition with leader epoch: $leaderEpoch.", e)

          return false
      }

      // Follower 日志
      val followerLog = localLogOrException
      val leaderEpochEndOffset = followerLog.logEndOffset
      stateChangeLogger.info(s"Follower $topicPartition starts at leader epoch ${partitionState.leaderEpoch} from " +
        s"offset $leaderEpochEndOffset with high watermark ${followerLog.highWatermark}. " +
        s"Previous leader epoch was $leaderEpoch.")

      // 记录新的 Leader epoch，清除前任 Leader 信息，更新 zkVersion 和 isLeader 属性
      leaderEpoch = partitionState.leaderEpoch
      leaderEpochStartOffsetOpt = None
      zkVersion = partitionState.zkVersion

      // Since we might have been a leader previously, still clear any pending AlterIsr requests
      // 清除任何挂起的 AlterIsr 请求，并检查副本状态
      alterIsrManager.clearPending(topicPartition)

      // 如果该副本之前就是 Leader，且被迫放弃领导权，返回 false；否则返回 true。
      if (leaderReplicaIdOpt.contains(newLeaderBrokerId) && leaderEpoch == oldLeaderEpoch) {
        false
      } else {
        // 重置 Leader 副本的 Broker ID
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the follower's state in the leader based on the last fetch request. See
   * [[Replica.updateFetchState()]] for details.
   *
   * @return true if the follower's fetch state was updated, false if the followerId is not recognized
   */
  def updateFollowerFetchState(followerId: Int,
                               followerFetchOffsetMetadata: LogOffsetMetadata,
                               followerStartOffset: Long,
                               followerFetchTimeMs: Long,
                               leaderEndOffset: Long): Boolean = {
    // 从 Partition#remoteReplicasMap 中获取副本对应的 Replica 实例。
    getReplica(followerId) match {
      case Some(followerReplica) =>
        // No need to calculate low watermark if there is no delayed DeleteRecordsRequest
        val oldLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
        val prevFollowerEndOffset = followerReplica.logEndOffset
        // 更新 Replica 实例，包括 logStartOffset、logEndOffsetMetadata 等属性。
        // 由于更新了 Follower 的 logEndOffset 属性，所以分区的高水位也可能需要更新。
        followerReplica.updateFetchState(
          followerFetchOffsetMetadata,
          followerStartOffset,
          followerFetchTimeMs,
          leaderEndOffset)

        // oldLeaderLW 、newLeaderLW 这两个变量是更新分区数据前后的低水位。
        // 这里的低水位是指：该分区所有副本中同步最落后的副本的 logStartOffset 偏移量。
        val newLeaderLW = if (delayedOperations.numDelayedDelete > 0) lowWatermarkIfLeader else -1L
        // check if the LW of the partition has incremented
        // since the replica's logStartOffset may have incremented
        // 更新分区高水位
        val leaderLWIncremented = newLeaderLW > oldLeaderLW

        // Check if this in-sync replica needs to be added to the ISR.
        // 扩容 ISR
        maybeExpandIsr(followerReplica, followerFetchTimeMs)

        // check if the HW of the partition can now be incremented
        // since the replica may already be in the ISR and its LEO has just incremented
        val leaderHWIncremented = if (prevFollowerEndOffset != followerReplica.logEndOffset) {
          leaderLogIfLocal.exists(leaderLog => maybeIncrementLeaderHW(leaderLog, followerFetchTimeMs))
        } else {
          false
        }

        // some delayed operations may be unblocked after HW or LW changed
        // 如果分区的低水位或者高水位发生了变化，则触发该分区生产者的延迟操作的正常结束行为。
        // 如果生产者设置了 acks = -1，则写入消息时，会创建一个延迟操作，
        // 该延迟操作需要等待 ISR 中所有副本同步数据后再返回成功响应给生产者。
        // 如果高水位发生变化则表示 ISR 中 Follower 副本同步了新数据，此时触发对应的生产者延迟操作的正常结束行为。
        if (leaderLWIncremented || leaderHWIncremented)
          tryCompleteDelayedRequests()

        debug(s"Recorded replica $followerId log end offset (LEO) position " +
          s"${followerFetchOffsetMetadata.messageOffset} and log start offset $followerStartOffset.")
        true

      case None =>
        false
    }
  }

  /**
   * Stores the topic partition assignment and ISR.
   * It creates a new Replica object for any new remote broker. The isr parameter is
   * expected to be a subset of the assignment parameter.
   *
   * Note: public visibility for tests.
   *
   * @param assignment An ordered sequence of all the broker ids that were assigned to this
   *                   topic partition
   * @param isr The set of broker ids that are known to be insync with the leader
   * @param addingReplicas An ordered sequence of all broker ids that will be added to the
    *                       assignment
   * @param removingReplicas An ordered sequence of all broker ids that will be removed from
    *                         the assignment
   */
  def updateAssignmentAndIsr(assignment: Seq[Int],
                             isr: Set[Int],
                             addingReplicas: Seq[Int],
                             removingReplicas: Seq[Int]): Unit = {
    val newRemoteReplicas = assignment.filter(_ != localBrokerId)
    val removedReplicas = remoteReplicasMap.keys.filter(!newRemoteReplicas.contains(_))

    // due to code paths accessing remoteReplicasMap without a lock,
    // first add the new replicas and then remove the old ones
    newRemoteReplicas.foreach(id => remoteReplicasMap.getAndMaybePut(id, new Replica(id, topicPartition)))
    remoteReplicasMap.removeAll(removedReplicas)

    if (addingReplicas.nonEmpty || removingReplicas.nonEmpty)
      assignmentState = OngoingReassignmentState(addingReplicas, removingReplicas, assignment)
    else
      assignmentState = SimpleAssignmentState(assignment)
    isrState = CommittedIsr(isr)
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * A replica will be added to ISR if its LEO >= current hw of the partition and it is caught up to
   * an offset within the current leader epoch. A replica must be caught up to the current leader
   * epoch before it can join ISR, because otherwise, if there is committed data between current
   * leader's HW and LEO, the replica may become the leader before it fetches the committed data
   * and the data will be lost.
   *
   * Technically, a replica shouldn't be in ISR if it hasn't caught up for longer than replicaLagTimeMaxMs,
   * even if its log end offset is >= HW. However, to be consistent with how the follower determines
   * whether a replica is in-sync, we only check HW.
   *
   * This function can be triggered when a replica's LEO has incremented.
   */
  private def maybeExpandIsr(followerReplica: Replica, followerFetchTimeMs: Long): Unit = {
    val needsIsrUpdate = canAddReplicaToIsr(followerReplica.brokerId) && inReadLock(leaderIsrUpdateLock) {
      needsExpandIsr(followerReplica)
    }
    if (needsIsrUpdate) {
      inWriteLock(leaderIsrUpdateLock) {
        // check if this replica needs to be added to the ISR
        if (needsExpandIsr(followerReplica)) {
          expandIsr(followerReplica.brokerId)
        }
      }
    }
  }

  private def needsExpandIsr(followerReplica: Replica): Boolean = {
    canAddReplicaToIsr(followerReplica.brokerId) && isFollowerAtHighwatermark(followerReplica)
  }

  private def canAddReplicaToIsr(followerReplicaId: Int): Boolean = {
    val current = isrState
    !current.isInflight && !current.isr.contains(followerReplicaId)
  }

  private def isFollowerAtHighwatermark(followerReplica: Replica): Boolean = {
    leaderLogIfLocal.exists { leaderLog =>
      val followerEndOffset = followerReplica.logEndOffset
      followerEndOffset >= leaderLog.highWatermark && leaderEpochStartOffsetOpt.exists(followerEndOffset >= _)
    }
  }

  /*
   * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
   * and the second element is an error (which would be `Errors.NONE` for no error).
   *
   * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
   * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
   * produce request.
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        // keep the current immutable replica list reference
        val curMaximalIsr = isrState.maximalIsr

        if (isTraceEnabled) {
          def logEndOffsetString: ((Int, Long)) => String = {
            case (brokerId, logEndOffset) => s"broker $brokerId: $logEndOffset"
          }

          val curInSyncReplicaObjects = (curMaximalIsr - localBrokerId).map(getReplicaOrException)
          val replicaInfo = curInSyncReplicaObjects.map(replica => (replica.brokerId, replica.logEndOffset))
          val localLogInfo = (localBrokerId, localLogOrException.logEndOffset)
          val (ackedReplicas, awaitingReplicas) = (replicaInfo + localLogInfo).partition { _._2 >= requiredOffset}

          trace(s"Progress awaiting ISR acks for offset $requiredOffset: " +
            s"acked: ${ackedReplicas.map(logEndOffsetString)}, " +
            s"awaiting ${awaitingReplicas.map(logEndOffsetString)}")
        }

        val minIsr = leaderLog.config.minInSyncReplicas
        if (leaderLog.highWatermark >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          if (minIsr <= curMaximalIsr.size)
            (true, Errors.NONE)
          else
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_OR_FOLLOWER)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
   * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
   * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
   * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
   * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
   * will never be added to ISR.
   *
   * With the addition of AlterIsr, we also consider newly added replicas as part of the ISR when advancing
   * the HW. These replicas have not yet been committed to the ISR by the controller, so we could revert to the previously
   * committed ISR. However, adding additional replicas to the ISR makes it more restrictive and therefor safe. We call
   * this set the "maximal" ISR. See KIP-497 for more details
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderLog: Log, curTime: Long = time.milliseconds): Boolean = {
    inReadLock(leaderIsrUpdateLock) {
      // maybeIncrementLeaderHW is in the hot path, the following code is written to
      // avoid unnecessary collection generation
      // 初始新的高水位线为 Leader 副本的 LEO
      var newHighWatermark = leaderLog.logEndOffsetMetadata
      // 遍历所有的远端副本
      remoteReplicasMap.values.foreach { replica =>
        // Note here we are using the "maximal", see explanation above
        // follower 副本在 ISR 副本集合中，follower 副本上次发送的 Fetch 请求距今时间小于 replicaLagTimeMaxMs
        // 属性值（因为这些 follower 副本后续会添加到 ISR 副本集合中，所以需要考虑这些 follower 副本），
        // 则将新高水位线更新为该副本的 LEO。
        if (replica.logEndOffsetMetadata.messageOffset < newHighWatermark.messageOffset &&
          (curTime - replica.lastCaughtUpTimeMs <= replicaLagTimeMaxMs || isrState.maximalIsr.contains(replica.brokerId))) {
          // follower 副本的最小 LEO 作为高水位值
          newHighWatermark = replica.logEndOffsetMetadata
        }
      }

      // 尝试通过新的高水位线更新 Leader 副本的高水位线
      leaderLog.maybeIncrementHighWatermark(newHighWatermark) match {
        case Some(oldHighWatermark) =>
          debug(s"High watermark updated from $oldHighWatermark to $newHighWatermark")
          // 更新成功
          true

        case None =>
          // 如果更新失败，打印日志以及当前的所有 LEO 信息
          def logEndOffsetString: ((Int, LogOffsetMetadata)) => String = {
            case (brokerId, logEndOffsetMetadata) => s"replica $brokerId: $logEndOffsetMetadata"
          }

          if (isTraceEnabled) {
            val replicaInfo = remoteReplicas.map(replica => (replica.brokerId, replica.logEndOffsetMetadata)).toSet
            val localLogInfo = (localBrokerId, localLogOrException.logEndOffsetMetadata)
            trace(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old value. " +
              s"All current LEOs are ${(replicaInfo + localLogInfo).map(logEndOffsetString)}")
          }
          // 更新失败
          false
      }
    }
  }

  /**
   * The low watermark offset value, calculated only if the local replica is the partition leader
   * It is only used by leader broker to decide when DeleteRecordsRequest is satisfied. Its value is minimum logStartOffset of all live replicas
   * Low watermark will increase when the leader broker receives either FetchRequest or DeleteRecordsRequest.
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeader)
      throw new NotLeaderOrFollowerException(s"Leader not local for partition $topicPartition on broker $localBrokerId")

    // lowWatermarkIfLeader may be called many times when a DeleteRecordsRequest is outstanding,
    // care has been taken to avoid generating unnecessary collections in this code
    var lowWaterMark = localLogOrException.logStartOffset
    remoteReplicas.foreach { replica =>
      if (metadataCache.getAliveBroker(replica.brokerId).nonEmpty && replica.logStartOffset < lowWaterMark) {
        lowWaterMark = replica.logStartOffset
      }
    }

    futureLog match {
      case Some(partitionFutureLog) =>
        Math.min(lowWaterMark, partitionFutureLog.logStartOffset)
      case None =>
        lowWaterMark
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests(): Unit = delayedOperations.checkAndCompleteAll()

  def maybeShrinkIsr(): Unit = {
    // 判断是否需要执行 ISR 收缩
    val needsIsrUpdate = !isrState.isInflight && inReadLock(leaderIsrUpdateLock) {
      needsShrinkIsr()
    }
    val leaderHWIncremented = needsIsrUpdate && inWriteLock(leaderIsrUpdateLock) {
      // 如果是 Leader 副本
      leaderLogIfLocal.exists { leaderLog =>
        // 获取不同步的副本 Id 列表
        val outOfSyncReplicaIds = getOutOfSyncReplicas(replicaLagTimeMaxMs)
        // 如果存在不同步的副本 Id 列表
        if (outOfSyncReplicaIds.nonEmpty) {
          // 获取不同步的的副本日志
          val outOfSyncReplicaLog = outOfSyncReplicaIds.map { replicaId =>
            s"(brokerId: $replicaId, endOffset: ${getReplicaOrException(replicaId).logEndOffset})"
          }.mkString(" ")
          // 计算收缩之后的 ISR 列表:把它们从当前 ISR 中剔除出去，然后计算得出最新的 ISR 列表
          val newIsrLog = (isrState.isr -- outOfSyncReplicaIds).mkString(",")
          info(s"Shrinking ISR from ${isrState.isr.mkString(",")} to $newIsrLog. " +
               s"Leader: (highWatermark: ${leaderLog.highWatermark}, endOffset: ${leaderLog.logEndOffset}). " +
               s"Out of sync replicas: $outOfSyncReplicaLog.")

          // 更新 ZooKeeper 中分区的 ISR 数据以及 Broker 的元数据缓存中的数据
          shrinkIsr(outOfSyncReplicaIds)

          // we may need to increment high watermark since ISR could be down to 1
          // 尝试更新 Leader 副本的高水位值
          maybeIncrementLeaderHW(leaderLog)
        } else {
          // 如果没有不同步的副本 Id 列表，什么都不做
          false
        }
      }
    }

    // some delayed operations may be unblocked after HW changed
    // 如果 Leader 副本的高水位值已经抬高了
    if (leaderHWIncremented)
    // 尝试解锁一下延迟请求
      tryCompleteDelayedRequests()
  }

  private def needsShrinkIsr(): Boolean = {
    // 获取与 Leader 不同步的副本
    leaderLogIfLocal.exists { _ => getOutOfSyncReplicas(replicaLagTimeMaxMs).nonEmpty }
  }

  private def isFollowerOutOfSync(replicaId: Int,
                                  leaderEndOffset: Long,
                                  currentTimeMs: Long,
                                  maxLagMs: Long): Boolean = {
    val followerReplica = getReplicaOrException(replicaId)
    followerReplica.logEndOffset != leaderEndOffset &&
      (currentTimeMs - followerReplica.lastCaughtUpTimeMs) > maxLagMs
  }

  /**
   * If the follower already has the same leo as the leader, it will not be considered as out-of-sync,
   * otherwise there are two cases that will be handled here -
   * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
   *                     the follower is stuck and should be removed from the ISR
   * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
   *                    then the follower is lagging and should be removed from the ISR
   * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
   * the last time when the replica was fully caught up. If either of the above conditions
   * is violated, that replica is considered to be out of sync
   *
   * If an ISR update is in-flight, we will return an empty set here
   **/
  def getOutOfSyncReplicas(maxLagMs: Long): Set[Int] = {
    val current = isrState
    if (!current.isInflight) {
      val candidateReplicaIds = current.isr - localBrokerId
      val currentTimeMs = time.milliseconds()
      val leaderEndOffset = localLogOrException.logEndOffset
      candidateReplicaIds.filter(replicaId => isFollowerOutOfSync(replicaId, leaderEndOffset, currentTimeMs, maxLagMs))
    } else {
      Set.empty
    }
  }

  private def doAppendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    if (isFuture) {
      // The read lock is needed to handle race condition if request handler thread tries to
      // remove future replica after receiving AlterReplicaLogDirsRequest.
      inReadLock(leaderIsrUpdateLock) {
        // Note the replica may be undefined if it is removed by a non-ReplicaAlterLogDirsThread before
        // this method is called
        futureLog.map { _.appendAsFollower(records) }
      }
    } else {
      // The lock is needed to prevent the follower replica from being updated while ReplicaAlterDirThread
      // is executing maybeReplaceCurrentWithFutureReplica() to replace follower replica with the future replica.
      futureLogLock.synchronized {
        Some(localLogOrException.appendAsFollower(records))
      }
    }
  }

  def appendRecordsToFollowerOrFutureReplica(records: MemoryRecords, isFuture: Boolean): Option[LogAppendInfo] = {
    try {
      doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
    } catch {
      case e: UnexpectedAppendOffsetException =>
        val log = if (isFuture) futureLocalLogOrException else localLogOrException
        val logEndOffset = log.logEndOffset
        if (logEndOffset == log.logStartOffset &&
            e.firstOffset < logEndOffset && e.lastOffset >= logEndOffset) {
          // This may happen if the log start offset on the leader (or current replica) falls in
          // the middle of the batch due to delete records request and the follower tries to
          // fetch its first offset from the leader.
          // We handle this case here instead of Log#append() because we will need to remove the
          // segment that start with log start offset and create a new one with earlier offset
          // (base offset of the batch), which will move recoveryPoint backwards, so we will need
          // to checkpoint the new recovery point before we append
          val replicaName = if (isFuture) "future replica" else "follower"
          info(s"Unexpected offset in append to $topicPartition. First offset ${e.firstOffset} is less than log start offset ${log.logStartOffset}." +
               s" Since this is the first record to be appended to the $replicaName's log, will start the log from offset ${e.firstOffset}.")
          truncateFullyAndStartAt(e.firstOffset, isFuture)
          doAppendRecordsToFollowerOrFutureReplica(records, isFuture)
        } else
          throw e
    }
  }

  def appendRecordsToLeader(records: MemoryRecords, origin: AppendOrigin, requiredAcks: Int): LogAppendInfo = {
    // 读锁
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      // 写之前先判断该 replica 是否是 Leader，如果不是 Leader 则没有写权限
      leaderLogIfLocal match {
        case Some(leaderLog) =>
          // 最小 isr 同步副本集合
          val minIsr = leaderLog.config.minInSyncReplicas
          // 当前 isr 同步副本集合数
          val inSyncSize = isrState.isr.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          // 如果请求的 acks = -1，但是当前的 ISR 比配置的 minInSyncReplicas 还小，那要抛出错误，表示当前 ISR 不足。
          if (inSyncSize < minIsr && requiredAcks == -1) {
            throw new NotEnoughReplicasException(s"The size of the current ISR ${isrState.isr} " +
              s"is insufficient to satisfy the min.isr requirement of $minIsr for partition $topicPartition")
          }

          // 消息通过 log 对象写入磁盘
          val info = leaderLog.appendAsLeader(records, leaderEpoch = this.leaderEpoch, origin,
            interBrokerProtocolVersion)

          // we may need to increment high watermark since ISR could be down to 1
          // 变更该分区的高水位
          (info, maybeIncrementLeaderHW(leaderLog))

        case None =>
          throw new NotLeaderOrFollowerException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    info.copy(leaderHwChange = if (leaderHWIncremented) LeaderHwChange.Increased else LeaderHwChange.Same)
  }

  def readRecords(lastFetchedEpoch: Optional[Integer], // 上一次获取的 epoch
                  fetchOffset: Long, //  获取记录时的偏移量
                  currentLeaderEpoch: Optional[Integer], // 当前 leader epoch
                  maxBytes: Int, //  最大获取字节数
                  fetchIsolation: FetchIsolation, // 消息隔离级别（读取未提交消息或全部消息）
                  fetchOnlyFromLeader: Boolean, // 是否只从 leader 读取
                  // 是否至少获取一条消息      // 这里通过读锁来读取
                  minOneMessage: Boolean): LogReadInfo = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    // 选择是否只从 leader 获取数据
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)

    // Note we use the log end offset prior to the read. This ensures that any appends following
    // the fetch do not prevent a follower from coming into sync.
    // 初始化位移值，注意，我们在读取记录之前使用日志结束偏移量 LEO。这将确保随后的追加操作不会阻止追随者同步。

    // 获取本地记录的 high watermark
    val initialHighWatermark = localLog.highWatermark
    // 获取本地记录的起始偏移量
    val initialLogStartOffset = localLog.logStartOffset
    // 获取本地记录的结束偏移量
    val initialLogEndOffset = localLog.logEndOffset
    // 获取本地记录的最后稳定偏移量，这里主要用于事务消息
    val initialLastStableOffset = localLog.lastStableOffset

    // lastFetchedEpoch 表示上一次同步条目的 epoch，如果指定了 lastFetchedEpoch，则查询该 epoch 的最后一个偏移量
    lastFetchedEpoch.ifPresent { fetchEpoch =>
      // 获取该 epoch 的最后一个偏移量信息
      val epochEndOffset = lastOffsetForLeaderEpoch(currentLeaderEpoch, fetchEpoch, fetchOnlyFromLeader = false)
      // 如果有错误，则抛出异常
      if (epochEndOffset.error != Errors.NONE) {
        throw epochEndOffset.error.exception()
      }

      // 如果最后一个偏移量未定义，则抛出 OffsetOutOfRangeException 异常
      if (epochEndOffset.hasUndefinedEpochOrOffset) {
        throw new OffsetOutOfRangeException("Could not determine the end offset of the last fetched epoch " +
          s"$lastFetchedEpoch from the request")
      }

      // 如果返回的 leader epoch 小于 fetchEpoch 或者返回的偏移量小于 fetchOffset，则返回空记录
      if (epochEndOffset.leaderEpoch < fetchEpoch || epochEndOffset.endOffset < fetchOffset) {
        // 定义空记录数据结构
        val emptyFetchData = FetchDataInfo(
          fetchOffsetMetadata = LogOffsetMetadata(fetchOffset),
          records = MemoryRecords.EMPTY,
          firstEntryIncomplete = false,
          abortedTransactions = None
        )

        // 生成分界点信息
        val divergingEpoch = new FetchResponseData.EpochEndOffset()
          .setEpoch(epochEndOffset.leaderEpoch)
          .setEndOffset(epochEndOffset.endOffset)

        // 返回 LogReadInfo，包含空记录和分界点信息等
        return LogReadInfo(
          fetchedData = emptyFetchData,
          divergingEpoch = Some(divergingEpoch),
          highWatermark = initialHighWatermark,
          logStartOffset = initialLogStartOffset,
          logEndOffset = initialLogEndOffset,
          lastStableOffset = initialLastStableOffset)
      }
    }

    // 读取 Log 日志消息
    val fetchedData = localLog.read(fetchOffset, maxBytes, fetchIsolation, minOneMessage)

    // 返回 LogReadInfo，包含已读记录和位移值信息等
    LogReadInfo(
      fetchedData = fetchedData,
      divergingEpoch = None,
      highWatermark = initialHighWatermark,
      logStartOffset = initialLogStartOffset,
      logEndOffset = initialLogEndOffset,
      lastStableOffset = initialLastStableOffset)
  }

  def fetchOffsetForTimestamp(timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)

    val lastFetchableOffset = isolationLevel match {
      case Some(IsolationLevel.READ_COMMITTED) => localLog.lastStableOffset
      case Some(IsolationLevel.READ_UNCOMMITTED) => localLog.highWatermark
      case None => localLog.logEndOffset
    }

    val epochLogString = if (currentLeaderEpoch.isPresent) {
      s"epoch ${currentLeaderEpoch.get}"
    } else {
      "unknown epoch"
    }

    // Only consider throwing an error if we get a client request (isolationLevel is defined) and the start offset
    // is lagging behind the high watermark
    val maybeOffsetsError: Option[ApiException] = leaderEpochStartOffsetOpt
      .filter(epochStart => isolationLevel.isDefined && epochStart > localLog.highWatermark)
      .map(epochStart => Errors.OFFSET_NOT_AVAILABLE.exception(s"Failed to fetch offsets for " +
        s"partition $topicPartition with leader $epochLogString as this partition's " +
        s"high watermark (${localLog.highWatermark}) is lagging behind the " +
        s"start offset from the beginning of this epoch ($epochStart)."))

    def getOffsetByTimestamp: Option[TimestampAndOffset] = {
      logManager.getLog(topicPartition).flatMap(log => log.fetchOffsetByTimestamp(timestamp))
    }

    // If we're in the lagging HW state after a leader election, throw OffsetNotAvailable for "latest" offset
    // or for a timestamp lookup that is beyond the last fetchable offset.
    timestamp match {
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        maybeOffsetsError.map(e => throw e)
          .orElse(Some(new TimestampAndOffset(RecordBatch.NO_TIMESTAMP, lastFetchableOffset, Optional.of(leaderEpoch))))
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        getOffsetByTimestamp
      case _ =>
        getOffsetByTimestamp.filter(timestampAndOffset => timestampAndOffset.offset < lastFetchableOffset)
          .orElse(maybeOffsetsError.map(e => throw e))
    }
  }

  def fetchOffsetSnapshot(currentLeaderEpoch: Optional[Integer],
                          fetchOnlyFromLeader: Boolean): LogOffsetSnapshot = inReadLock(leaderIsrUpdateLock) {
    // decide whether to only fetch from leader
    val localLog = localLogWithEpochOrException(currentLeaderEpoch, fetchOnlyFromLeader)
    localLog.fetchOffsetSnapshot
  }

  def legacyFetchOffsetsForTimestamp(timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = inReadLock(leaderIsrUpdateLock) {
    val localLog = localLogWithEpochOrException(Optional.empty(), fetchOnlyFromLeader)
    val allOffsets = localLog.legacyFetchOffsetsBefore(timestamp, maxNumOffsets)

    if (!isFromConsumer) {
      allOffsets
    } else {
      val hw = localLog.highWatermark
      if (allOffsets.exists(_ > hw))
        hw +: allOffsets.dropWhile(_ > hw)
      else
        allOffsets
    }
  }

  def logStartOffset: Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderLogIfLocal.map(_.logStartOffset).getOrElse(-1)
    }
  }

  /**
   * Update logStartOffset and low watermark if 1) offset <= highWatermark and 2) it is the leader replica.
   * This function can trigger log segment deletion and log rolling.
   *
   * Return low watermark of the partition.
   */
  def deleteRecordsOnLeader(offset: Long): LogDeleteRecordsResult = inReadLock(leaderIsrUpdateLock) {
    leaderLogIfLocal match {
      case Some(leaderLog) =>
        if (!leaderLog.config.delete)
          throw new PolicyViolationException(s"Records of partition $topicPartition can not be deleted due to the configured policy")

        val convertedOffset = if (offset == DeleteRecordsRequest.HIGH_WATERMARK)
          leaderLog.highWatermark
        else
          offset

        if (convertedOffset < 0)
          throw new OffsetOutOfRangeException(s"The offset $convertedOffset for partition $topicPartition is not valid")

        leaderLog.maybeIncrementLogStartOffset(convertedOffset, ClientRecordDeletion)
        LogDeleteRecordsResult(
          requestedOffset = convertedOffset,
          lowWatermark = lowWatermarkIfLeader)
      case None =>
        throw new NotLeaderOrFollowerException(s"Leader not local for partition $topicPartition on broker $localBrokerId")
    }
  }

  /**
    * Truncate the local log of this partition to the specified offset and checkpoint the recovery point to this offset
    *
    * @param offset offset to be used for truncation
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateTo(offset: Long, isFuture: Boolean): Unit = {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeReplaceCurrentWithFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateTo(Map(topicPartition -> offset), isFuture = isFuture)
    }
  }

  /**
    * Delete all data in the local log of this partition and start the log at the new offset
    *
    * @param newOffset The new offset to start the log with
    * @param isFuture True iff the truncation should be performed on the future log of this partition
    */
  def truncateFullyAndStartAt(newOffset: Long, isFuture: Boolean): Unit = {
    // The read lock is needed to prevent the follower replica from being truncated while ReplicaAlterDirThread
    // is executing maybeReplaceCurrentWithFutureReplica() to replace follower replica with the future replica.
    inReadLock(leaderIsrUpdateLock) {
      logManager.truncateFullyAndStartAt(topicPartition, newOffset, isFuture = isFuture)
    }
  }

  /**
   * Find the (exclusive) last offset of the largest epoch less than or equal to the requested epoch.
   *
   * @param currentLeaderEpoch The expected epoch of the current leader (if known)
   * @param leaderEpoch Requested leader epoch
   * @param fetchOnlyFromLeader Whether or not to require servicing only from the leader
   *
   * @return The requested leader epoch and the end offset of this leader epoch, or if the requested
   *         leader epoch is unknown, the leader epoch less than the requested leader epoch and the end offset
   *         of this leader epoch. The end offset of a leader epoch is defined as the start
   *         offset of the first leader epoch larger than the leader epoch, or else the log end
   *         offset if the leader epoch is the latest leader epoch.
   */
  def lastOffsetForLeaderEpoch(currentLeaderEpoch: Optional[Integer],
                               leaderEpoch: Int,
                               fetchOnlyFromLeader: Boolean): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      val localLogOrError = getLocalLog(currentLeaderEpoch, fetchOnlyFromLeader)
      localLogOrError match {
        case Left(localLog) =>
          localLog.endOffsetForEpoch(leaderEpoch) match {
            case Some(epochAndOffset) => new EpochEndOffset(NONE, epochAndOffset.leaderEpoch, epochAndOffset.offset)
            case None => new EpochEndOffset(NONE, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
          }
        case Right(error) =>
          new EpochEndOffset(error, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  // 把 isr 的改变通知给 Controller 节点，然后 Controller 节点再把 isr 的信息通知给其他的 broker节点
  private[cluster] def expandIsr(newInSyncReplica: Int): Unit = {
    if (useAlterIsr) {
      expandIsrWithAlterIsr(newInSyncReplica)
    } else {
      expandIsrWithZk(newInSyncReplica)
    }
  }

  private def expandIsrWithAlterIsr(newInSyncReplica: Int): Unit = {
    // This is called from maybeExpandIsr which holds the ISR write lock
    if (!isrState.isInflight) {
      // When expanding the ISR, we can safely assume the new replica will make it into the ISR since this puts us in
      // a more constrained state for advancing the HW.
      sendAlterIsrRequest(PendingExpandIsr(isrState.isr, newInSyncReplica))
    } else {
      trace(s"ISR update in-flight, not adding new in-sync replica $newInSyncReplica")
    }
  }

  private def expandIsrWithZk(newInSyncReplica: Int): Unit = {
    val newInSyncReplicaIds = isrState.isr + newInSyncReplica
    info(s"Expanding ISR from ${isrState.isr.mkString(",")} to ${newInSyncReplicaIds.mkString(",")}")
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newInSyncReplicaIds.toList, zkVersion)
    val zkVersionOpt = stateStore.expandIsr(controllerEpoch, newLeaderAndIsr)
    if (zkVersionOpt.isDefined) {
      isrChangeListener.markExpand()
    }
    maybeUpdateIsrAndVersionWithZk(newInSyncReplicaIds, zkVersionOpt)
  }

  private[cluster] def shrinkIsr(outOfSyncReplicas: Set[Int]): Unit = {
    if (useAlterIsr) {
      shrinkIsrWithAlterIsr(outOfSyncReplicas)
    } else {
      shrinkIsrWithZk(isrState.isr -- outOfSyncReplicas)
    }
  }

  private def shrinkIsrWithAlterIsr(outOfSyncReplicas: Set[Int]): Unit = {
    // This is called from maybeShrinkIsr which holds the ISR write lock
    if (!isrState.isInflight) {
      // When shrinking the ISR, we cannot assume that the update will succeed as this could erroneously advance the HW
      // We update pendingInSyncReplicaIds here simply to prevent any further ISR updates from occurring until we get
      // the next LeaderAndIsr
      sendAlterIsrRequest(PendingShrinkIsr(isrState.isr, outOfSyncReplicas))
    } else {
      trace(s"ISR update in-flight, not removing out-of-sync replicas $outOfSyncReplicas")
    }
  }

  private def shrinkIsrWithZk(newIsr: Set[Int]): Unit = {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.toList, zkVersion)
    val zkVersionOpt = stateStore.shrinkIsr(controllerEpoch, newLeaderAndIsr)
    if (zkVersionOpt.isDefined) {
      isrChangeListener.markShrink()
    }
    maybeUpdateIsrAndVersionWithZk(newIsr, zkVersionOpt)
  }

  private def maybeUpdateIsrAndVersionWithZk(isr: Set[Int], zkVersionOpt: Option[Int]): Unit = {
    zkVersionOpt match {
      case Some(newVersion) =>
        isrState = CommittedIsr(isr)
        zkVersion = newVersion
        info("ISR updated to [%s] and zkVersion updated to [%d]".format(isr.mkString(","), zkVersion))

      case None =>
        info(s"Cached zkVersion $zkVersion not equal to that in zookeeper, skip updating ISR")
        isrChangeListener.markFailed()
    }
  }

  private def sendAlterIsrRequest(proposedIsrState: IsrState): Unit = {
    // 构建要发送的 isr 信息集合
    val isrToSend: Set[Int] = proposedIsrState match {
      case PendingExpandIsr(isr, newInSyncReplicaId) => isr + newInSyncReplicaId
      case PendingShrinkIsr(isr, outOfSyncReplicaIds) => isr -- outOfSyncReplicaIds
      case state =>
        throw new IllegalStateException(s"Invalid state $state for `AlterIsr` request for partition $topicPartition")
    }

    // 需要把 Leader 信息和 isr 信息整合在一起发送
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, isrToSend.toList, zkVersion)
    // 进一步完善发送信息，增加 isr 对应的主题分区
    val alterIsrItem = AlterIsrItem(topicPartition, newLeaderAndIsr, handleAlterIsrResponse(proposedIsrState))

    if (!alterIsrManager.enqueue(alterIsrItem)) {
      isrChangeListener.markFailed()
      throw new IllegalStateException(s"Failed to enqueue `AlterIsr` request with state " +
        s"$newLeaderAndIsr for partition $topicPartition")
    }

    isrState = proposedIsrState
    debug(s"Sent `AlterIsr` request to change state to $newLeaderAndIsr after transition to $proposedIsrState")
  }

  /**
   * This is called for each partition in the body of an AlterIsr response. For errors which are non-retryable we simply
   * give up. This leaves [[Partition.isrState]] in an in-flight state (either pending shrink or pending expand).
   * Since our error was non-retryable we are okay staying in this state until we see new metadata from UpdateMetadata
   * or LeaderAndIsr
   */
  private def handleAlterIsrResponse(proposedIsrState: IsrState)(result: Either[Errors, LeaderAndIsr]): Unit = {
    inWriteLock(leaderIsrUpdateLock) {
      if (isrState != proposedIsrState) {
        // This means isrState was updated through leader election or some other mechanism before we got the AlterIsr
        // response. We don't know what happened on the controller exactly, but we do know this response is out of date
        // so we ignore it.
        debug(s"Ignoring failed ISR update to $proposedIsrState since we have already updated state to $isrState")
        return
      }

      result match {
        case Left(error: Errors) =>
          isrChangeListener.markFailed()
          error match {
            case Errors.UNKNOWN_TOPIC_OR_PARTITION =>
              debug(s"Controller failed to update ISR to $proposedIsrState since it doesn't know about this topic or partition. Giving up.")
            case Errors.FENCED_LEADER_EPOCH =>
              debug(s"Controller failed to update ISR to $proposedIsrState since we sent an old leader epoch. Giving up.")
            case Errors.INVALID_UPDATE_VERSION =>
              debug(s"Controller failed to update ISR to $proposedIsrState due to invalid zk version. Giving up.")
            case _ =>
              warn(s"Controller failed to update ISR to $proposedIsrState due to unexpected $error. Retrying.")
              sendAlterIsrRequest(proposedIsrState)
          }
        case Right(leaderAndIsr: LeaderAndIsr) =>
          // Success from controller, still need to check a few things
          if (leaderAndIsr.leaderEpoch != leaderEpoch) {
            debug(s"Ignoring ISR from AlterIsr with ${leaderAndIsr} since we have a stale leader epoch $leaderEpoch.")
            isrChangeListener.markFailed()
          } else if (leaderAndIsr.zkVersion <= zkVersion) {
            debug(s"Ignoring ISR from AlterIsr with ${leaderAndIsr} since we have a newer version $zkVersion.")
            isrChangeListener.markFailed()
          } else {
            isrState = CommittedIsr(leaderAndIsr.isr.toSet)
            zkVersion = leaderAndIsr.zkVersion
            info(s"ISR updated from AlterIsr to ${isrState.isr.mkString(",")} and version updated to [$zkVersion]")
            proposedIsrState match {
              case PendingExpandIsr(_, _) => isrChangeListener.markExpand()
              case PendingShrinkIsr(_, _) => isrChangeListener.markShrink()
              case _ => // nothing to do, shouldn't get here
            }
          }
      }
    }
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId

  override def toString: String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; Replicas: " + assignmentState.replicas.mkString(","))
    partitionString.append("; ISR: " + isrState.isr.mkString(","))
    assignmentState match {
      case OngoingReassignmentState(adding, removing, _) =>
        partitionString.append("; AddingReplicas: " + adding.mkString(","))
        partitionString.append("; RemovingReplicas: " + removing.mkString(","))
      case _ =>
    }
    partitionString.toString
  }
}
