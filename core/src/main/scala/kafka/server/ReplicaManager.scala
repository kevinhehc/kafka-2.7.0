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
package kafka.server

import java.io.File
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.api._
import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.common.RecordValidationException
import kafka.controller.{KafkaController, StateChangeLogger}
import kafka.log._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{FetchMetadata => SFetchMetadata}
import kafka.server.HostedPartition.Online
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.checkpoints.{LazyOffsetCheckpoints, OffsetCheckpointFile, OffsetCheckpoints}
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{ElectionType, IsolationLevel, Node, TopicPartition}
import org.apache.kafka.common.errors._
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.message.{DescribeLogDirsResponseData, FetchResponseData, LeaderAndIsrResponseData}
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError
import org.apache.kafka.common.message.StopReplicaRequestData.StopReplicaPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset
import org.apache.kafka.common.record._
import org.apache.kafka.common.replica.PartitionView.DefaultPartitionView
import org.apache.kafka.common.replica.ReplicaView.DefaultReplicaView
import org.apache.kafka.common.replica.{ClientMetadata, _}
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, Set, mutable}
import scala.compat.java8.OptionConverters._

/*
 * Result metadata of a log append operation on the log
 */
case class LogAppendResult(info: LogAppendInfo, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

case class LogDeleteRecordsResult(requestedOffset: Long, lowWatermark: Long, exception: Option[Throwable] = None) {
  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }
}

/**
 * Result metadata of a log read operation on the log
 * @param info @FetchDataInfo returned by the @Log read
 * @param divergingEpoch Optional epoch and end offset which indicates the largest epoch such
 *                       that subsequent records are known to diverge on the follower/consumer
 * @param highWatermark high watermark of the local replica
 * @param leaderLogStartOffset The log start offset of the leader at the time of the read
 * @param leaderLogEndOffset The log end offset of the leader at the time of the read
 * @param followerLogStartOffset The log start offset of the follower taken from the Fetch request
 * @param fetchTimeMs The time the fetch was received
 * @param lastStableOffset Current LSO or None if the result has an exception
 * @param preferredReadReplica the preferred read replica to be used for future fetches
 * @param exception Exception if error encountered while reading from the log
 */
case class LogReadResult(info: FetchDataInfo,
                         divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                         highWatermark: Long,
                         leaderLogStartOffset: Long,
                         leaderLogEndOffset: Long,
                         followerLogStartOffset: Long,
                         fetchTimeMs: Long,
                         lastStableOffset: Option[Long],
                         preferredReadReplica: Option[Int] = None,
                         exception: Option[Throwable] = None) {

  def error: Errors = exception match {
    case None => Errors.NONE
    case Some(e) => Errors.forException(e)
  }

  def withEmptyFetchInfo: LogReadResult =
    copy(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY))

  override def toString = {
    "LogReadResult(" +
      s"info=$info, " +
      s"divergingEpoch=$divergingEpoch, " +
      s"highWatermark=$highWatermark, " +
      s"leaderLogStartOffset=$leaderLogStartOffset, " +
      s"leaderLogEndOffset=$leaderLogEndOffset, " +
      s"followerLogStartOffset=$followerLogStartOffset, " +
      s"fetchTimeMs=$fetchTimeMs, " +
      s"preferredReadReplica=$preferredReadReplica, " +
      s"lastStableOffset=$lastStableOffset, " +
      s"error=$error" +
      ")"
  }

}

case class FetchPartitionData(error: Errors = Errors.NONE,
                              highWatermark: Long,
                              logStartOffset: Long,
                              records: Records,
                              divergingEpoch: Option[FetchResponseData.EpochEndOffset],
                              lastStableOffset: Option[Long],
                              abortedTransactions: Option[List[AbortedTransaction]],
                              preferredReadReplica: Option[Int],
                              isReassignmentFetch: Boolean)


/**
 * Trait to represent the state of hosted partitions. We create a concrete (active) Partition
 * instance when the broker receives a LeaderAndIsr request from the controller indicating
 * that it should be either a leader or follower of a partition.
 */
sealed trait HostedPartition
object HostedPartition {
  /**
   * This broker does not have any state for this partition locally.
   */
  final object None extends HostedPartition

  /**
   * This broker hosts the partition and it is online.
   */
  final case class Online(partition: Partition) extends HostedPartition

  /**
   * This broker hosts the partition, but it is in an offline log directory.
   */
  final object Offline extends HostedPartition
}

case class IsrChangePropagationConfig(
  // How often to check for ISR
  checkIntervalMs: Long,

  // Maximum time that an ISR change may be delayed before sending the notification
  maxDelayMs: Long,

  // Maximum time to await additional changes before sending the notification
  lingerMs: Long
)

object ReplicaManager {
  val HighWatermarkFilename = "replication-offset-checkpoint"

  // This field is mutable to allow overriding change notification behavior in test cases
  @volatile var DefaultIsrPropagationConfig: IsrChangePropagationConfig = IsrChangePropagationConfig(
    checkIntervalMs = 2500,
    lingerMs = 5000,
    maxDelayMs = 60000,
  )
}

class ReplicaManager(val config: KafkaConfig, // 配置管理类
                     metrics: Metrics, // 监控指标类
                     time: Time, // 定时器类
                     val zkClient: KafkaZkClient,   // ZooKeeper 客户端
                     scheduler: Scheduler, // Kafka调度器
                     val logManager: LogManager, // 日志管理器
                     val isShuttingDown: AtomicBoolean, // 是否已经关闭
                     quotaManagers: QuotaManagers, // 配额管理器
                     val brokerTopicStats: BrokerTopicStats, // Broker主题监控指标
                     val metadataCache: MetadataCache, // Broker 元数据缓存
                     logDirFailureChannel: LogDirFailureChannel,
                     // 处理延时 PRODUCE 请求的 Purgatory
                     val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                     // 处理延时 FETCH 请求的 Purgatory
                     val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                     // 处理延时 DELETE_RECORDS 请求的 Purgatory
                     val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords],
                     // 处理延时 ELECT_LEADERS 请求的 Purgatory
                     val delayedElectLeaderPurgatory: DelayedOperationPurgatory[DelayedElectLeader],
                     threadNamePrefix: Option[String],
                     val alterIsrManager: AlterIsrManager) extends Logging with KafkaMetricsGroup {

  def this(config: KafkaConfig,
           metrics: Metrics,
           time: Time,
           zkClient: KafkaZkClient,
           scheduler: Scheduler,
           logManager: LogManager,
           isShuttingDown: AtomicBoolean,
           quotaManagers: QuotaManagers,
           brokerTopicStats: BrokerTopicStats,
           metadataCache: MetadataCache,
           logDirFailureChannel: LogDirFailureChannel,
           alterIsrManager: AlterIsrManager,
           threadNamePrefix: Option[String] = None) = {
    this(config, metrics, time, zkClient, scheduler, logManager, isShuttingDown,
      quotaManagers, brokerTopicStats, metadataCache, logDirFailureChannel,
      DelayedOperationPurgatory[DelayedProduce](
        purgatoryName = "Produce", brokerId = config.brokerId,
        purgeInterval = config.producerPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedFetch](
        purgatoryName = "Fetch", brokerId = config.brokerId,
        purgeInterval = config.fetchPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedDeleteRecords](
        purgatoryName = "DeleteRecords", brokerId = config.brokerId,
        purgeInterval = config.deleteRecordsPurgatoryPurgeIntervalRequests),
      DelayedOperationPurgatory[DelayedElectLeader](
        purgatoryName = "ElectLeader", brokerId = config.brokerId),
      threadNamePrefix, alterIsrManager)
  }

  /* epoch of the controller that last changed the leader */
  @volatile var controllerEpoch: Int = KafkaController.InitialControllerEpoch
  private val localBrokerId = config.brokerId
  private val allPartitions = new Pool[TopicPartition, HostedPartition](
    valueFactory = Some(tp => HostedPartition.Online(Partition(tp, time, this)))
  )
  private val replicaStateChangeLock = new Object
  val replicaFetcherManager = createReplicaFetcherManager(metrics, time, threadNamePrefix, quotaManagers.follower)
  val replicaAlterLogDirsManager = createReplicaAlterLogDirsManager(quotaManagers.alterLogDirs, brokerTopicStats)
  private val highWatermarkCheckPointThreadStarted = new AtomicBoolean(false)
  @volatile var highWatermarkCheckpoints: Map[String, OffsetCheckpointFile] = logManager.liveLogDirs.map(dir =>
    (dir.getAbsolutePath, new OffsetCheckpointFile(new File(dir, ReplicaManager.HighWatermarkFilename), logDirFailureChannel))).toMap

  this.logIdent = s"[ReplicaManager broker=$localBrokerId] "
  private val stateChangeLogger = new StateChangeLogger(localBrokerId, inControllerContext = false, None)

  private val isrChangeNotificationConfig = ReplicaManager.DefaultIsrPropagationConfig
  private val isrChangeSet: mutable.Set[TopicPartition] = new mutable.HashSet[TopicPartition]()
  private val lastIsrChangeMs = new AtomicLong(time.milliseconds())
  private val lastIsrPropagationMs = new AtomicLong(time.milliseconds())

  private var logDirFailureHandler: LogDirFailureHandler = null

  private class LogDirFailureHandler(name: String, haltBrokerOnDirFailure: Boolean) extends ShutdownableThread(name) {
    override def doWork(): Unit = {
      val newOfflineLogDir = logDirFailureChannel.takeNextOfflineLogDir()
      if (haltBrokerOnDirFailure) {
        fatal(s"Halting broker because dir $newOfflineLogDir is offline")
        Exit.halt(1)
      }
      handleLogDirFailure(newOfflineLogDir)
    }
  }

  // Visible for testing
  private[server] val replicaSelectorOpt: Option[ReplicaSelector] = createReplicaSelector()

  newGauge("LeaderCount", () => leaderPartitionsIterator.size)
  // Visible for testing
  private[kafka] val partitionCount = newGauge("PartitionCount", () => allPartitions.size)
  newGauge("OfflineReplicaCount", () => offlinePartitionCount)
  newGauge("UnderReplicatedPartitions", () => underReplicatedPartitionCount)
  newGauge("UnderMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isUnderMinIsr))
  newGauge("AtMinIsrPartitionCount", () => leaderPartitionsIterator.count(_.isAtMinIsr))
  newGauge("ReassigningPartitions", () => reassigningPartitionsCount)

  def reassigningPartitionsCount: Int = leaderPartitionsIterator.count(_.isReassigning)

  val isrExpandRate: Meter = newMeter("IsrExpandsPerSec", "expands", TimeUnit.SECONDS)
  val isrShrinkRate: Meter = newMeter("IsrShrinksPerSec", "shrinks", TimeUnit.SECONDS)
  val failedIsrUpdatesRate: Meter = newMeter("FailedIsrUpdatesPerSec", "failedUpdates", TimeUnit.SECONDS)

  def underReplicatedPartitionCount: Int = leaderPartitionsIterator.count(_.isUnderReplicated)

  def startHighWatermarkCheckPointThread(): Unit = {
    if (highWatermarkCheckPointThreadStarted.compareAndSet(false, true))
      scheduler.schedule("highwatermark-checkpoint", checkpointHighWatermarks _, period = config.replicaHighWatermarkCheckpointIntervalMs, unit = TimeUnit.MILLISECONDS)
  }

  def recordIsrChange(topicPartition: TopicPartition): Unit = {
    if (!config.interBrokerProtocolVersion.isAlterIsrSupported) {
      isrChangeSet synchronized {
        isrChangeSet += topicPartition
        lastIsrChangeMs.set(time.milliseconds())
      }
    }
  }
  /**
   * This function periodically runs to see if ISR needs to be propagated. It propagates ISR when:
   * 1. There is ISR change not propagated yet.
   * 2. There is no ISR Change in the last five seconds, or it has been more than 60 seconds since the last ISR propagation.
   * This allows an occasional ISR change to be propagated within a few seconds, and avoids overwhelming controller and
   * other brokers when large amount of ISR change occurs.
   */
  def maybePropagateIsrChanges(): Unit = {
    val now = time.milliseconds()
    isrChangeSet synchronized {
      if (isrChangeSet.nonEmpty &&
        (lastIsrChangeMs.get() + isrChangeNotificationConfig.lingerMs < now ||
          lastIsrPropagationMs.get() + isrChangeNotificationConfig.maxDelayMs < now)) {
        zkClient.propagateIsrChanges(isrChangeSet)
        isrChangeSet.clear()
        lastIsrPropagationMs.set(now)
      }
    }
  }

  // When ReplicaAlterDirThread finishes replacing a current replica with a future replica, it will
  // remove the partition from the partition state map. But it will not close itself even if the
  // partition state map is empty. Thus we need to call shutdownIdleReplicaAlterDirThread() periodically
  // to shutdown idle ReplicaAlterDirThread
  def shutdownIdleReplicaAlterLogDirsThread(): Unit = {
    replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
  }

  def getLog(topicPartition: TopicPartition): Option[Log] = logManager.getLog(topicPartition)

  def hasDelayedElectionOperations: Boolean = delayedElectLeaderPurgatory.numDelayed != 0

  def tryCompleteElection(key: DelayedOperationKey): Unit = {
    val completed = delayedElectLeaderPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d ElectLeader.".format(key.keyLabel, completed))
  }

  def startup(): Unit = {
    // start ISR expiration thread
    // A follower can lag behind leader for up to config.replicaLagTimeMaxMs x 1.5 before it is removed from ISR
    scheduler.schedule("isr-expiration", maybeShrinkIsr _, period = config.replicaLagTimeMaxMs / 2, unit = TimeUnit.MILLISECONDS)
    // If using AlterIsr, we don't need the znode ISR propagation
    if (!config.interBrokerProtocolVersion.isAlterIsrSupported) {
      scheduler.schedule("isr-change-propagation", maybePropagateIsrChanges _,
        period = isrChangeNotificationConfig.checkIntervalMs, unit = TimeUnit.MILLISECONDS)
    } else {
      alterIsrManager.start()
    }
    scheduler.schedule("shutdown-idle-replica-alter-log-dirs-thread", shutdownIdleReplicaAlterLogDirsThread _, period = 10000L, unit = TimeUnit.MILLISECONDS)

    // If inter-broker protocol (IBP) < 1.0, the controller will send LeaderAndIsrRequest V0 which does not include isNew field.
    // In this case, the broker receiving the request cannot determine whether it is safe to create a partition if a log directory has failed.
    // Thus, we choose to halt the broker on any log diretory failure if IBP < 1.0
    val haltBrokerOnFailure = config.interBrokerProtocolVersion < KAFKA_1_0_IV0
    logDirFailureHandler = new LogDirFailureHandler("LogDirFailureHandler", haltBrokerOnFailure)
    logDirFailureHandler.start()
  }

  private def maybeRemoveTopicMetrics(topic: String): Unit = {
    val topicHasOnlinePartition = allPartitions.values.exists {
      case HostedPartition.Online(partition) => topic == partition.topic
      case HostedPartition.None | HostedPartition.Offline => false
    }
    if (!topicHasOnlinePartition)
      brokerTopicStats.removeMetrics(topic)
  }

  private def completeDelayedFetchOrProduceRequests(topicPartition: TopicPartition): Unit = {
    val topicPartitionOperationKey = TopicPartitionOperationKey(topicPartition)
    delayedProducePurgatory.checkAndComplete(topicPartitionOperationKey)
    delayedFetchPurgatory.checkAndComplete(topicPartitionOperationKey)
  }

  def stopReplicas(correlationId: Int,
                   controllerId: Int,
                   controllerEpoch: Int,
                   brokerEpoch: Long,
                   partitionStates: Map[TopicPartition, StopReplicaPartitionState]
                  ): (mutable.Map[TopicPartition, Errors], Errors) = {
    replicaStateChangeLock synchronized {
      stateChangeLogger.info(s"Handling StopReplica request correlationId $correlationId from controller " +
        s"$controllerId for ${partitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        partitionStates.forKeyValue { (topicPartition, partitionState) =>
          stateChangeLogger.trace(s"Received StopReplica request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch $controllerEpoch for partition $topicPartition")
        }

      val responseMap = new collection.mutable.HashMap[TopicPartition, Errors]
      if (controllerEpoch < this.controllerEpoch) {
        stateChangeLogger.warn(s"Ignoring StopReplica request from " +
          s"controller $controllerId with correlation id $correlationId " +
          s"since its controller epoch $controllerEpoch is old. " +
          s"Latest known controller epoch is ${this.controllerEpoch}")
        (responseMap, Errors.STALE_CONTROLLER_EPOCH)
      } else {
        // stopReplicas方法处理StopReplicaRequest请求时
        this.controllerEpoch = controllerEpoch

        val stoppedPartitions = mutable.Map.empty[TopicPartition, StopReplicaPartitionState]
        partitionStates.forKeyValue { (topicPartition, partitionState) =>
          val deletePartition = partitionState.deletePartition

          getPartition(topicPartition) match {
            case HostedPartition.Offline =>
              stateChangeLogger.warn(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
              // 添加对象异常到 Response
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)

            case HostedPartition.Online(partition) =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              // When a topic is deleted, the leader epoch is not incremented. To circumvent this,
              // a sentinel value (EpochDuringDelete) overwriting any previous epoch is used.
              // When an older version of the StopReplica request which does not contain the leader
              // epoch, a sentinel value (NoEpoch) is used and bypass the epoch validation.
              if (requestLeaderEpoch == LeaderAndIsr.EpochDuringDelete ||
                  requestLeaderEpoch == LeaderAndIsr.NoEpoch ||
                  requestLeaderEpoch > currentLeaderEpoch) {
                stoppedPartitions += topicPartition -> partitionState
                // Assume that everything will go right. It is overwritten in case of an error.
                responseMap.put(topicPartition, Errors.NONE)
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                stateChangeLogger.warn(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              } else {
                stateChangeLogger.info(s"Ignoring StopReplica request (delete=$deletePartition) from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                responseMap.put(topicPartition, Errors.FENCED_LEADER_EPOCH)
              }

            case HostedPartition.None =>
              // Delete log and corresponding folders in case replica manager doesn't hold them anymore.
              // This could happen when topic is being deleted while broker is down and recovers.
              stoppedPartitions += topicPartition -> partitionState
              responseMap.put(topicPartition, Errors.NONE)
          }
        }

        // First stop fetchers for all partitions.
        val partitions = stoppedPartitions.keySet
        replicaFetcherManager.removeFetcherForPartitions(partitions)
        replicaAlterLogDirsManager.removeFetcherForPartitions(partitions)

        // Second remove deleted partitions from the partition map. Fetchers rely on the
        // ReplicaManager to get Partition's information so they must be stopped first.
        val deletedPartitions = mutable.Set.empty[TopicPartition]
        stoppedPartitions.forKeyValue { (topicPartition, partitionState) =>
          if (partitionState.deletePartition) {
            getPartition(topicPartition) match {
              case hostedPartition@HostedPartition.Online(partition) =>
                if (allPartitions.remove(topicPartition, hostedPartition)) {
                  maybeRemoveTopicMetrics(topicPartition.topic)
                  // Logs are not deleted here. They are deleted in a single batch later on.
                  // This is done to avoid having to checkpoint for every deletions.
                  partition.delete()
                }

              case _ =>
            }

            deletedPartitions += topicPartition
          }

          // If we were the leader, we may have some operations still waiting for completion.
          // We force completion to prevent them from timing out.
          completeDelayedFetchOrProduceRequests(topicPartition)
        }

        // Third delete the logs and checkpoint.
        logManager.asyncDelete(deletedPartitions, (topicPartition, exception) => {
          exception match {
            case e: KafkaStorageException =>
              stateChangeLogger.error(s"Ignoring StopReplica request (delete=true) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                "partition is in an offline log directory")
              responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)

            case e =>
              stateChangeLogger.error(s"Ignoring StopReplica request (delete=true) from " +
                s"controller $controllerId with correlation id $correlationId " +
                s"epoch $controllerEpoch for partition $topicPartition due to an unexpected " +
                s"${e.getClass.getName} exception: ${e.getMessage}")
              responseMap.put(topicPartition, Errors.forException(e))
          }
        })

        (responseMap, Errors.NONE)
      }
    }
  }

  def getPartition(topicPartition: TopicPartition): HostedPartition = {
    Option(allPartitions.get(topicPartition)).getOrElse(HostedPartition.None)
  }

  def isAddingReplica(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    getPartition(topicPartition) match {
      case Online(partition) => partition.isAddingReplica(replicaId)
      case _ => false
    }
  }

  // Visible for testing
  def createPartition(topicPartition: TopicPartition): Partition = {
    val partition = Partition(topicPartition, time, this)
    allPartitions.put(topicPartition, HostedPartition.Online(partition))
    partition
  }

  def nonOfflinePartition(topicPartition: TopicPartition): Option[Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) => Some(partition)
      case HostedPartition.None | HostedPartition.Offline => None
    }
  }

  // An iterator over all non offline partitions. This is a weakly consistent iterator; a partition made offline after
  // the iterator has been constructed could still be returned by this iterator.
  private def nonOfflinePartitionsIterator: Iterator[Partition] = {
    allPartitions.values.iterator.flatMap {
      case HostedPartition.Online(partition) => Some(partition)
      case HostedPartition.None | HostedPartition.Offline => None
    }
  }

  private def offlinePartitionCount: Int = {
    allPartitions.values.iterator.count(_ == HostedPartition.Offline)
  }

  def getPartitionOrException(topicPartition: TopicPartition): Partition = {
    getPartitionOrError(topicPartition) match {
      case Left(Errors.KAFKA_STORAGE_ERROR) =>
        throw new KafkaStorageException(s"Partition $topicPartition is in an offline log directory")

      case Left(error) =>
        throw error.exception(s"Error while fetching partition state for $topicPartition")

      case Right(partition) => partition
    }
  }

  def getPartitionOrError(topicPartition: TopicPartition): Either[Errors, Partition] = {
    getPartition(topicPartition) match {
      case HostedPartition.Online(partition) =>
        Right(partition)

      case HostedPartition.Offline =>
        Left(Errors.KAFKA_STORAGE_ERROR)

      case HostedPartition.None if metadataCache.contains(topicPartition) =>
        // The topic exists, but this broker is no longer a replica of it, so we return NOT_LEADER_OR_FOLLOWER which
        // forces clients to refresh metadata to find the new location. This can happen, for example,
        // during a partition reassignment if a produce request from the client is sent to a broker after
        // the local replica has been deleted.
        Left(Errors.NOT_LEADER_OR_FOLLOWER)

      case HostedPartition.None =>
        Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
    }
  }

  def localLogOrException(topicPartition: TopicPartition): Log = {
    getPartitionOrException(topicPartition).localLogOrException
  }

  def futureLocalLogOrException(topicPartition: TopicPartition): Log = {
    getPartitionOrException(topicPartition).futureLocalLogOrException
  }

  def futureLogExists(topicPartition: TopicPartition): Boolean = {
    getPartitionOrException(topicPartition).futureLog.isDefined
  }

  def localLog(topicPartition: TopicPartition): Option[Log] = {
    nonOfflinePartition(topicPartition).flatMap(_.log)
  }

  def getLogDir(topicPartition: TopicPartition): Option[String] = {
    localLog(topicPartition).map(_.parentDir)
  }

  /**
   * TODO: move this action queue to handle thread so we can simplify concurrency handling
   */
  private val actionQueue = new ActionQueue

  def tryCompleteActions(): Unit = actionQueue.tryCompleteActions()

  /**
   * Append messages to leader replicas of the partition, and wait for them to be replicated to other replicas;
   * the callback function will be triggered either when timeout or the required acks are satisfied;
   * if the callback function itself is already synchronized on some object then pass this object to avoid deadlock.
   *
   * Noted that all pending delayed check operations are stored in a queue. All callers to ReplicaManager.appendRecords()
   * are expected to call ActionQueue.tryCompleteActions for all affected partitions, without holding any conflicting
   * locks.
   */
  // 追加日志消息
  def appendRecords(timeout: Long, // 请求处理超时时间。对于生产者来说，它就是 request.timeout.ms 参数值。
                    requiredAcks: Short,  // 是否需要等待其他副本写入。对于生产者来说，它就是 acks 参数的值。
                    internalTopicsAllowed: Boolean, // 是否允许向内部主题写入消息。对于普通的生产者而言，该字段是 false，
                                                    // 即不允许写入内部主题。而对于 Coordinator 组件，
                                                    // 特别是消费者组 GroupCoordinator 组件来说，
                                                    // 它的职责之一就是向内部位移主题写入消息，此时该字段值是 true。
                    origin: AppendOrigin, // 是一个接口，表示写入方来源。当前，它定义了3类写入方，分别是Replication、Coordinator和Client
                    entriesPerPartition: Map[TopicPartition, MemoryRecords], // 按照分区分组的实际要写入的消息二元组集合
                    responseCallback: Map[TopicPartition, PartitionResponse] => Unit, // 写入成功后的回调方法
                    delayedProduceLock: Option[Lock] = None, // 专门用来保护消费者组操作线程安全的锁对象
                    recordConversionStatsCallback: Map[TopicPartition, RecordConversionStats] => Unit = _ => ()): Unit = {
    // 校验 ack 合法值，如果非法直接返回错误响应
    if (isValidRequiredAcks(requiredAcks)) {
      val sTime = time.milliseconds
      // 调用 appendToLocalLog 方法写入消息到本地日志文件中，返回写入结果
      val localProduceResults = appendToLocalLog(internalTopicsAllowed = internalTopicsAllowed,
        origin, entriesPerPartition, requiredAcks)
      debug("Produce to local log in %d ms".format(time.milliseconds - sTime))

      // 根据写日志返回的结果，封装返回给客户端的响应结果。
      val produceStatus = localProduceResults.map { case (topicPartition, result) =>
        topicPartition ->
                ProducePartitionStatus(
                  // 计算出下一条待写入消息的位移值
                  result.info.lastOffset + 1, // required offset
                  // 构建 PartitionResponse 封装写入结果
                  new PartitionResponse(result.error, result.info.firstOffset.getOrElse(-1), result.info.logAppendTime,
                    result.info.logStartOffset, result.info.recordErrors.asJava, result.info.errorMessage)) // response status
      }

      // 尝试触发操作
      actionQueue.add {
        () =>
          localProduceResults.foreach {
            case (topicPartition, result) =>
              val requestKey = TopicPartitionOperationKey(topicPartition)
              result.info.leaderHwChange match {
                case LeaderHwChange.Increased =>
                  // some delayed operations may be unblocked after HW changed
                  // 一些延迟操作或许因为 HW 更改而被解除阻塞
                  delayedProducePurgatory.checkAndComplete(requestKey)
                  delayedFetchPurgatory.checkAndComplete(requestKey)
                  delayedDeleteRecordsPurgatory.checkAndComplete(requestKey)
                case LeaderHwChange.Same =>
                  // 一些跟随者获取请求可能会因为日志末尾偏移量更新而解除阻塞
                  // probably unblock some follower fetch requests since log end offset has been updated
                  delayedFetchPurgatory.checkAndComplete(requestKey)
                case LeaderHwChange.None =>
                // 什么也不做
                  // nothing
              }
          }
      }

      // 尝试更新消息格式转换的指标数据
      recordConversionStatsCallback(localProduceResults.map { case (k, v) => k -> v.info.recordConversionStats })

      // 根据 acks 的值，判断是否需要延迟响应客户端。通常当 acks = -1 时，
      // 需要通知其他 replica 复制消息，保证复制完了才会响应客户端，这时候就需要使用延迟操作，并构建对应的 DelayedProduce 对象
      if (delayedProduceRequestRequired(requiredAcks, entriesPerPartition, localProduceResults)) {
        // create delayed produce operation
        // 构造 ProduceMetadata 对象，记录要写入的消息状态
        val produceMetadata = ProduceMetadata(requiredAcks, produceStatus)
        // 构造 DelayedProduce 延迟请求对象，对写入结果进行异步等待。超过timeout时间后这个操作会被认定为超时，并立刻返回，发送响应给客户端。
        val delayedProduce = new DelayedProduce(timeout, produceMetadata, this, responseCallback, delayedProduceLock)

        // create a list of (topic, partition) pairs to use as keys for this delayed produce operation
        // 构建一个由 (topic, partition) 对列表，用来作为 DelayedProduce 延时请求对象的 key，然后遍历有哪些 topic 需要检查延迟操作是否完成。
        val producerRequestKeys = entriesPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

        // try to complete the request immediately, otherwise put it into the purgatory
        // this is because while the delayed produce operation is being created, new
        // requests may arrive and hence make this operation completable.
        // 尝试立即响应请求，否则将其压入 Purgatory 中等待唤醒后处理
        delayedProducePurgatory.tryCompleteElseWatch(delayedProduce, producerRequestKeys)

      } else {
        // we can respond immediately
        // 不需要等待响应立刻发送响应给客户端，直接将 produceStatus 转换成 response 内存块返回接收到的请求线程
        val produceResponseStatus = produceStatus.map { case (k, status) => k -> status.responseStatus }
        // 调用回调方法
        responseCallback(produceResponseStatus)
      }
    } else {
      // If required.acks is outside accepted range, something is wrong with the client
      // Just return an error and don't handle the request at all
      // 如果 required.acks 的值非法，说明客户端使用异常，直接返回错误响应
      val responseStatus = entriesPerPartition.map { case (topicPartition, _) =>
        topicPartition -> new PartitionResponse(Errors.INVALID_REQUIRED_ACKS,
          LogAppendInfo.UnknownLogAppendInfo.firstOffset.getOrElse(-1), RecordBatch.NO_TIMESTAMP, LogAppendInfo.UnknownLogAppendInfo.logStartOffset)
      }
      // 调用回调方法
      responseCallback(responseStatus)
    }
  }

  /**
   * Delete records on leader replicas of the partition, and wait for delete records operation be propagated to other replicas;
   * the callback function will be triggered either when timeout or logStartOffset of all live replicas have reached the specified offset
   */
  private def deleteRecordsOnLocalLog(offsetPerPartition: Map[TopicPartition, Long]): Map[TopicPartition, LogDeleteRecordsResult] = {
    trace("Delete records on local logs to offsets [%s]".format(offsetPerPartition))
    offsetPerPartition.map { case (topicPartition, requestedOffset) =>
      // reject delete records operation on internal topics
      if (Topic.isInternal(topicPartition.topic)) {
        (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(new InvalidTopicException(s"Cannot delete records of internal topic ${topicPartition.topic}"))))
      } else {
        try {
          val partition = getPartitionOrException(topicPartition)
          val logDeleteResult = partition.deleteRecordsOnLeader(requestedOffset)
          (topicPartition, logDeleteResult)
        } catch {
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: OffsetOutOfRangeException |
                   _: PolicyViolationException |
                   _: KafkaStorageException) =>
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(e)))
          case t: Throwable =>
            error("Error processing delete records operation on partition %s".format(topicPartition), t)
            (topicPartition, LogDeleteRecordsResult(-1L, -1L, Some(t)))
        }
      }
    }
  }

  // If there exists a topic partition that meets the following requirement,
  // we need to put a delayed DeleteRecordsRequest and wait for the delete records operation to complete
  //
  // 1. the delete records operation on this partition is successful
  // 2. low watermark of this partition is smaller than the specified offset
  private def delayedDeleteRecordsRequired(localDeleteRecordsResults: Map[TopicPartition, LogDeleteRecordsResult]): Boolean = {
    localDeleteRecordsResults.exists{ case (_, deleteRecordsResult) =>
      deleteRecordsResult.exception.isEmpty && deleteRecordsResult.lowWatermark < deleteRecordsResult.requestedOffset
    }
  }

  /**
   * For each pair of partition and log directory specified in the map, if the partition has already been created on
   * this broker, move its log files to the specified log directory. Otherwise, record the pair in the memory so that
   * the partition will be created in the specified log directory when broker receives LeaderAndIsrRequest for the partition later.
   */
  def alterReplicaLogDirs(partitionDirs: Map[TopicPartition, String]): Map[TopicPartition, Errors] = {
    replicaStateChangeLock synchronized {
      partitionDirs.map { case (topicPartition, destinationDir) =>
        try {
          /* If the topic name is exceptionally long, we can't support altering the log directory.
           * See KAFKA-4893 for details.
           * TODO: fix this by implementing topic IDs. */
          if (Log.logFutureDirName(topicPartition).size > 255)
            throw new InvalidTopicException("The topic name is too long.")
          if (!logManager.isLogDirOnline(destinationDir))
            throw new KafkaStorageException(s"Log directory $destinationDir is offline")

          getPartition(topicPartition) match {
            case HostedPartition.Online(partition) =>
              // Stop current replica movement if the destinationDir is different from the existing destination log directory
              if (partition.futureReplicaDirChanged(destinationDir)) {
                replicaAlterLogDirsManager.removeFetcherForPartitions(Set(topicPartition))
                partition.removeFutureLocalReplica()
              }
            case HostedPartition.Offline =>
              throw new KafkaStorageException(s"Partition $topicPartition is offline")

            case HostedPartition.None => // Do nothing
          }

          // If the log for this partition has not been created yet:
          // 1) Record the destination log directory in the memory so that the partition will be created in this log directory
          //    when broker receives LeaderAndIsrRequest for this partition later.
          // 2) Respond with NotLeaderOrFollowerException for this partition in the AlterReplicaLogDirsResponse
          logManager.maybeUpdatePreferredLogDir(topicPartition, destinationDir)

          // throw NotLeaderOrFollowerException if replica does not exist for the given partition
          val partition = getPartitionOrException(topicPartition)
          partition.localLogOrException

          // If the destinationLDir is different from the current log directory of the replica:
          // - If there is no offline log directory, create the future log in the destinationDir (if it does not exist) and
          //   start ReplicaAlterDirThread to move data of this partition from the current log to the future log
          // - Otherwise, return KafkaStorageException. We do not create the future log while there is offline log directory
          //   so that we can avoid creating future log for the same partition in multiple log directories.
          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          if (partition.maybeCreateFutureReplica(destinationDir, highWatermarkCheckpoints)) {
            val futureLog = futureLocalLogOrException(topicPartition)
            logManager.abortAndPauseCleaning(topicPartition)

            val initialFetchState = InitialFetchState(BrokerEndPoint(config.brokerId, "localhost", -1),
              partition.getLeaderEpoch, futureLog.highWatermark)
            replicaAlterLogDirsManager.addFetcherForPartitions(Map(topicPartition -> initialFetchState))
          }

          (topicPartition, Errors.NONE)
        } catch {
          case e@(_: InvalidTopicException |
                  _: LogDirNotFoundException |
                  _: ReplicaNotAvailableException |
                  _: KafkaStorageException) =>
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.forException(e))
          case e: NotLeaderOrFollowerException =>
            // Retaining REPLICA_NOT_AVAILABLE exception for ALTER_REPLICA_LOG_DIRS for compatibility
            warn(s"Unable to alter log dirs for $topicPartition", e)
            (topicPartition, Errors.REPLICA_NOT_AVAILABLE)
          case t: Throwable =>
            error("Error while changing replica dir for partition %s".format(topicPartition), t)
            (topicPartition, Errors.forException(t))
        }
      }
    }
  }

  /*
   * Get the LogDirInfo for the specified list of partitions.
   *
   * Each LogDirInfo specifies the following information for a given log directory:
   * 1) Error of the log directory, e.g. whether the log is online or offline
   * 2) size and lag of current and future logs for each partition in the given log directory. Only logs of the queried partitions
   *    are included. There may be future logs (which will replace the current logs of the partition in the future) on the broker after KIP-113 is implemented.
   */
  def describeLogDirs(partitions: Set[TopicPartition]): List[DescribeLogDirsResponseData.DescribeLogDirsResult] = {
    val logsByDir = logManager.allLogs.groupBy(log => log.parentDir)

    config.logDirs.toSet.map { logDir: String =>
      val absolutePath = new File(logDir).getAbsolutePath
      try {
        if (!logManager.isLogDirOnline(absolutePath))
          throw new KafkaStorageException(s"Log directory $absolutePath is offline")

        logsByDir.get(absolutePath) match {
          case Some(logs) =>
            val topicInfos = logs.groupBy(_.topicPartition.topic).map{case (topic, logs) =>
              new DescribeLogDirsResponseData.DescribeLogDirsTopic().setName(topic).setPartitions(
                logs.filter { log =>
                  partitions.contains(log.topicPartition)
                }.map { log =>
                  new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                    .setPartitionSize(log.size)
                    .setPartitionIndex(log.topicPartition.partition)
                    .setOffsetLag(getLogEndOffsetLag(log.topicPartition, log.logEndOffset, log.isFuture))
                    .setIsFutureKey(log.isFuture)
                }.toList.asJava)
            }.toList.asJava

            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code).setTopics(topicInfos)
          case None =>
            new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir(absolutePath)
              .setErrorCode(Errors.NONE.code)
        }

      } catch {
        case e: KafkaStorageException =>
          warn("Unable to describe replica dirs for %s".format(absolutePath), e)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.KAFKA_STORAGE_ERROR.code)
        case t: Throwable =>
          error(s"Error while describing replica in dir $absolutePath", t)
          new DescribeLogDirsResponseData.DescribeLogDirsResult()
            .setLogDir(absolutePath)
            .setErrorCode(Errors.forException(t).code)
      }
    }.toList
  }

  def getLogEndOffsetLag(topicPartition: TopicPartition, logEndOffset: Long, isFuture: Boolean): Long = {
    localLog(topicPartition) match {
      case Some(log) =>
        if (isFuture)
          log.logEndOffset - logEndOffset
        else
          math.max(log.highWatermark - logEndOffset, 0)
      case None =>
        // return -1L to indicate that the LEO lag is not available if the replica is not created or is offline
        DescribeLogDirsResponse.INVALID_OFFSET_LAG
    }
  }

  def deleteRecords(timeout: Long,
                    offsetPerPartition: Map[TopicPartition, Long],
                    responseCallback: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit): Unit = {
    val timeBeforeLocalDeleteRecords = time.milliseconds
    val localDeleteRecordsResults = deleteRecordsOnLocalLog(offsetPerPartition)
    debug("Delete records on local log in %d ms".format(time.milliseconds - timeBeforeLocalDeleteRecords))

    val deleteRecordsStatus = localDeleteRecordsResults.map { case (topicPartition, result) =>
      topicPartition ->
        DeleteRecordsPartitionStatus(
          result.requestedOffset, // requested offset
          new DeleteRecordsPartitionResult()
            .setLowWatermark(result.lowWatermark)
            .setErrorCode(result.error.code)
            .setPartitionIndex(topicPartition.partition)) // response status
    }

    if (delayedDeleteRecordsRequired(localDeleteRecordsResults)) {
      // create delayed delete records operation
      val delayedDeleteRecords = new DelayedDeleteRecords(timeout, deleteRecordsStatus, this, responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed delete records operation
      val deleteRecordsRequestKeys = offsetPerPartition.keys.map(TopicPartitionOperationKey(_)).toSeq

      // try to complete the request immediately, otherwise put it into the purgatory
      // this is because while the delayed delete records operation is being created, new
      // requests may arrive and hence make this operation completable.
      delayedDeleteRecordsPurgatory.tryCompleteElseWatch(delayedDeleteRecords, deleteRecordsRequestKeys)
    } else {
      // we can respond immediately
      val deleteRecordsResponseStatus = deleteRecordsStatus.map { case (k, status) => k -> status.responseStatus }
      responseCallback(deleteRecordsResponseStatus)
    }
  }

  // If all the following conditions are true, we need to put a delayed produce request and wait for replication to complete
  //
  // 1. required acks = -1
  // 2. there is data to append
  // 3. at least one partition append was successful (fewer errors than partitions)
  private def delayedProduceRequestRequired(requiredAcks: Short,
                                            entriesPerPartition: Map[TopicPartition, MemoryRecords],
                                            localProduceResults: Map[TopicPartition, LogAppendResult]): Boolean = {
    requiredAcks == -1 &&
    entriesPerPartition.nonEmpty &&
    localProduceResults.values.count(_.exception.isDefined) < entriesPerPartition.size
  }

  private def isValidRequiredAcks(requiredAcks: Short): Boolean = {
    requiredAcks == -1 || requiredAcks == 1 || requiredAcks == 0
  }

  /**
   * Append the messages to the local replica logs
   */
  // 将消息追加到本地 Leader 副本的日志中
  private def appendToLocalLog(internalTopicsAllowed: Boolean, // 是否允许写入内部主题
                               origin: AppendOrigin, // 写入来源
                               entriesPerPartition: Map[TopicPartition, MemoryRecords], // 按照分区分组的实际要写入的消息二元组集合
                               requiredAcks: Short): Map[TopicPartition, LogAppendResult] = {
    val traceEnabled = isTraceEnabled
    // 当追加日志操作失败，则记录日志、发送统计数据，并返回产生错误的分区的起始偏移量
    def processFailedRecord(topicPartition: TopicPartition, t: Throwable) = {
      val logStartOffset = getPartition(topicPartition) match {
        case HostedPartition.Online(partition) => partition.logStartOffset
        case HostedPartition.None | HostedPartition.Offline => -1L
      }
      brokerTopicStats.topicStats(topicPartition.topic).failedProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.failedProduceRequestRate.mark()
      error(s"Error processing append operation on partition $topicPartition", t)

      logStartOffset
    }

    if (traceEnabled)
      trace(s"Append [$entriesPerPartition] to local log")

    // 遍历 entriesPerPartition 分区中的每个分区对应的消息数据 (topicPartition, records) 对象，然后一个个分区写消息集合。
    entriesPerPartition.map { case (topicPartition, records) =>
      // 统计请求的产生总量、失败总量
      brokerTopicStats.topicStats(topicPartition.topic).totalProduceRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalProduceRequestRate.mark()

      // reject appending to internal topics if it is not allowed
      // 如果当前分区 topicPartition 是系统内部主题 __consumer_offsets，且在配置中被设置不允许追加操作，则追加操作被拒绝
      // 内部主题是：topic:__consumer_offsets 和 __transaction_state
      if (Topic.isInternal(topicPartition.topic) && !internalTopicsAllowed) {
        (topicPartition, LogAppendResult(
          LogAppendInfo.UnknownLogAppendInfo,
          // 直接返回客户端错误信息
          Some(new InvalidTopicException(s"Cannot append to internal topic ${topicPartition.topic}"))))
      } else {
        // 正常业务的 topic 执行逻辑
        try {
          // 获取 topicPartition 对应的日志分区对象 partition
          val partition = getPartitionOrException(topicPartition)
          // 调用 partition 类的方法向 Leader 分区对象写入消息集合
          val info = partition.appendRecordsToLeader(records, origin, requiredAcks)
          // 计算写入的消息数量
          val numAppendedMessages = info.numMessages

          // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
          // 根据追加的数据计算更新部分统计信息，例如每秒追加的字节数 bytesInRate、每秒追加的消息总量 messagesInRate。
          brokerTopicStats.topicStats(topicPartition.topic).bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.allTopicsStats.bytesInRate.mark(records.sizeInBytes)
          brokerTopicStats.topicStats(topicPartition.topic).messagesInRate.mark(numAppendedMessages)
          brokerTopicStats.allTopicsStats.messagesInRate.mark(numAppendedMessages)

          if (traceEnabled)
            trace(s"${records.sizeInBytes} written to log $topicPartition beginning at offset " +
              s"${info.firstOffset.getOrElse(-1)} and ending at offset ${info.lastOffset}")

          // 返回封装追加结果 LogAppendResult 对象
          (topicPartition, LogAppendResult(info))
        } catch {
          // NOTE: Failed produce requests metric is not incremented for known exceptions
          // it is supposed to indicate un-expected failures of a broker in handling a produce request
          // 处理已知异常，例如未找到分区、非 Leader 副本、记录过大、批次条目过大、记录校验失败、记录数据损坏等异常，
          // 一旦追加失败，将为异常记录统计数据、打印日志，并返回相应的 LogAppendResult 对象
          case e@ (_: UnknownTopicOrPartitionException |
                   _: NotLeaderOrFollowerException |
                   _: RecordTooLargeException |
                   _: RecordBatchTooLargeException |
                   _: CorruptRecordException |
                   _: KafkaStorageException) =>
            (topicPartition, LogAppendResult(LogAppendInfo.UnknownLogAppendInfo, Some(e)))
          case rve: RecordValidationException =>
            val logStartOffset = processFailedRecord(topicPartition, rve.invalidException)
            val recordErrors = rve.recordErrors
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithAdditionalInfo(
              logStartOffset, recordErrors, rve.invalidException.getMessage), Some(rve.invalidException)))
          case t: Throwable =>
            val logStartOffset = processFailedRecord(topicPartition, t)
            (topicPartition, LogAppendResult(LogAppendInfo.unknownLogAppendInfoWithLogStartOffset(logStartOffset), Some(t)))
        }
      }
    }
  }

  def fetchOffsetForTimestamp(topicPartition: TopicPartition,
                              timestamp: Long,
                              isolationLevel: Option[IsolationLevel],
                              currentLeaderEpoch: Optional[Integer],
                              fetchOnlyFromLeader: Boolean): Option[TimestampAndOffset] = {
    val partition = getPartitionOrException(topicPartition)
    partition.fetchOffsetForTimestamp(timestamp, isolationLevel, currentLeaderEpoch, fetchOnlyFromLeader)
  }

  def legacyFetchOffsetsForTimestamp(topicPartition: TopicPartition,
                                     timestamp: Long,
                                     maxNumOffsets: Int,
                                     isFromConsumer: Boolean,
                                     fetchOnlyFromLeader: Boolean): Seq[Long] = {
    val partition = getPartitionOrException(topicPartition)
    partition.legacyFetchOffsetsForTimestamp(timestamp, maxNumOffsets, isFromConsumer, fetchOnlyFromLeader)
  }

  /**
   * Fetch messages from a replica, and wait until enough data can be fetched and return;
   * the callback function will be triggered either when timeout or required fetch info is satisfied.
   * Consumers may fetch from any replica, but followers can only fetch from the leader.
   */
  // Broker 端接收到该请求后，调用 fetchMessages 方法从底层的 Leader 副本取出消息。
  // 需要等待足够的数据被获取后返回，回调函数将在超时或满足所需的拉取信息时触发。
  // 消费者可以从任何副本中拉取消息，但 follower 者只能从 Leader 拉取。
  def fetchMessages(timeout: Long, // 拉取处理超时时间，对于消费者而言，该值就是 request.timeout.ms 参数值。
                    // 对于 Follower 副本而言，该值是 Broker 端参数 replica.fetch.wait.max.ms 的值。
                    replicaId: Int, // 副本ID。对于消费者而言，该参数值是-1；对于 Follower 副本而言，该值就是 Follower 副本所在的Broker ID。
                    fetchMinBytes: Int, // 能够拉取请求的最小字节数。对于消费者而言，对应于 Consumer 端参数 fetch.min.bytes 值；
                    // 对于 Follower 副本而言，对应于 Broker 端参数replica.fetch.min.bytes 值。
                    fetchMaxBytes: Int, // 能够拉取请求的最大字节数。对于消费者而言，对应于 Consumer 端参数 fetch.max.bytes 值；
                    // 对于 Follower 副本而言，对应于 Broker 端参数replica.fetch.max.bytes 值。
                    hardMaxBytesLimit: Boolean, // 是否严格限制 fetchMaxBytes。
                    fetchInfos: Seq[(TopicPartition, PartitionData)], // 拉取请求的详细信息
                    // （包括拉取哪些 Partition、从这些分区的哪个起始偏移量开始读、最大可以拉取多少字节数等）。
                    quota: ReplicaQuota,  // 配额控制类，主要是为了判断是否需要在读取的过程中做限速控制
                    responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit,// 回调逻辑函数，当请求被处理完成后，调用该方法执行收尾逻辑。
                    isolationLevel: IsolationLevel, // 用于设置消息读取的事务隔离级别。
                    clientMetadata: Option[ClientMetadata]): Unit = { // 用于判断消息拉取是基于消费端还是基于 Leader 端。
    // 判断该读取请求是否来自于 Follower 副本或 Consumer
    val isFromFollower = Request.isValidBrokerId(replicaId)
    val isFromConsumer = !(isFromFollower || replicaId == Request.FutureLocalReplicaId)
    // fetchIsolation 即读取隔离级别，根据请求发送方判断可读取范围
    val fetchIsolation = if (!isFromConsumer)
      FetchLogEnd

    // 如果请求来自于Follower副本，那么可以读到LEO值，即从分区最大的 Offset 开始拉取
    else if (isolationLevel == IsolationLevel.READ_COMMITTED)
      FetchTxnCommitted

    // 如果请求是普通消费者,且来自于配置了READ_COMMITTED的消费者，那么可以读到Log Stable Offset值，即从已提交的事务开始拉取
    else
    // 其他消费者，那么可以读到高水位值前的消息
      FetchHighWatermark

    // Restrict fetching to leader if request is from follower or from a client with older version (no ClientMetadata)
    // 如果是从节点或者消息客户端版本太老，就限制只能从 Leader 获取数据（fetchOnlyFromLeader），
    // 否则可以从任意节点拉取消息数据，读取的过程中会记录剩余配额和统计 Kafka 服务器的相关指标。
    val fetchOnlyFromLeader = isFromFollower || (isFromConsumer && clientMetadata.isEmpty)

    // 定义读取本地日志中的消息的方法。
    def readFromLog(): Seq[(TopicPartition, LogReadResult)] = {
      // 从本地副本中读取指定 Partition 的消息数据
      val result = readFromLocalLog(
        replicaId = replicaId,
        fetchOnlyFromLeader = fetchOnlyFromLeader,
        fetchIsolation = fetchIsolation,
        fetchMaxBytes = fetchMaxBytes,
        hardMaxBytesLimit = hardMaxBytesLimit,
        readPartitionInfo = fetchInfos,
        quota = quota,
        clientMetadata = clientMetadata)

      // 如果拉取方是 follower 副本节点，则可能需要更新分区的高水位
      if (isFromFollower) updateFollowerFetchState(replicaId, result)
      // 否则直接返回读取结果。
      else result
    }

    // 调用读取本地日志的方法，并返回读取日志的结果。
    // logReadResults 是 Seq[(TopicPartition, LogReadResult)]
    val logReadResults = readFromLog()
    // 对读取结果进行一些统计，如统计不同 Topic 的拉取速度，记录已读取的字节数等。
    // check if this fetch request can be satisfied right away
    var bytesReadable: Long = 0
    var errorReadingData = false
    var hasDivergingEpoch = false
    val logReadResultMap = new mutable.HashMap[TopicPartition, LogReadResult]
    logReadResults.foreach { case (topicPartition, logReadResult) =>
      brokerTopicStats.topicStats(topicPartition.topic).totalFetchRequestRate.mark()
      brokerTopicStats.allTopicsStats.totalFetchRequestRate.mark()

      if (logReadResult.error != Errors.NONE)
        errorReadingData = true
      if (logReadResult.divergingEpoch.nonEmpty)
        hasDivergingEpoch = true
      bytesReadable = bytesReadable + logReadResult.info.records.sizeInBytes
      logReadResultMap.put(topicPartition, logReadResult)
    }

    // respond immediately if 1) fetch request does not want to wait
    //                        2) fetch request does not require any data
    //                        3) has enough data to respond
    //                        4) some error happens while reading data
    //                        5) we found a diverging epoch

    // 4、根据读取日志的结果，判断是否要立即返回给客户端
    //  如果符合以下任一条件：
    //      1）不需要等待即 fetch 请求未设置等待时间；
    //      2）fetch 请求内容为空；
    //      3）已经读取到的数据量达到了 fetchMinBytes 即读取数据量满足 fetch 请求 ；
    //      4）在读取数据时出现了错误；
    //      5）发现了新的  LeaderEpoch 时，那么就立即将拉取到的消息数据封装成 FetchPartitionData 对象，
    //      并回调 responseCallback 函数返回给消费方。否则，构建一个延迟拉取对象 delayedFetch，
    //      将拉取请求暂时放到等待队列中，等待符合上述任一条件时再将消息数据返回给消费方。
    if (timeout <= 0 || fetchInfos.isEmpty || bytesReadable >= fetchMinBytes || errorReadingData || hasDivergingEpoch) {
      val fetchPartitionData = logReadResults.map { case (tp, result) =>
        val isReassignmentFetch = isFromFollower && isAddingReplica(tp, replicaId)
        tp -> FetchPartitionData(
          result.error,
          result.highWatermark,
          result.leaderLogStartOffset,
          result.info.records,
          result.divergingEpoch,
          result.lastStableOffset,
          result.info.abortedTransactions,
          result.preferredReadReplica,
          isReassignmentFetch)
      }
      // 执行回调
      responseCallback(fetchPartitionData)
    } else {
      // construct the fetch results from the read results
      // 如果无法立即完成请求,就要延迟处理了
      // construct the fetch results from the read results
      // 构建 fetchPartitionStatus 数据结构，并将每个 Partition 的起始偏移量等信息和 PartitionData 数据结构一起打包
      // （例如 topic，partition，maxBytes 等），存储在 SFetchMetadata 中。
      val fetchPartitionStatus = new mutable.ArrayBuffer[(TopicPartition, FetchPartitionStatus)]
      fetchInfos.foreach { case (topicPartition, partitionData) =>
        logReadResultMap.get(topicPartition).foreach(logReadResult => {
          val logOffsetMetadata = logReadResult.info.fetchOffsetMetadata
          fetchPartitionStatus += (topicPartition -> FetchPartitionStatus(logOffsetMetadata, partitionData))
        })
      }
      val fetchMetadata: SFetchMetadata = SFetchMetadata(fetchMinBytes, fetchMaxBytes, hardMaxBytesLimit,
        fetchOnlyFromLeader, fetchIsolation, isFromFollower, replicaId, fetchPartitionStatus)
      val delayedFetch = new DelayedFetch(timeout, fetchMetadata, this, quota, clientMetadata,
        responseCallback)

      // create a list of (topic, partition) pairs to use as keys for this delayed fetch operation
      // 构建一组 Topic / Partition 键值对，用于在延迟拉取队列中暂存该拉取请求。如果此时还能够满足消费端的拉取请求，则会直接返回数据；
      // 否则，会将请求进一步延迟，直到能够获取到符合条件的消息数据后才会返回给消费端。
      val delayedFetchKeys = fetchPartitionStatus.map { case (tp, _) => TopicPartitionOperationKey(tp) }

      // try to complete the request immediately, otherwise put it into the purgatory;
      // this is because while the delayed fetch operation is being created, new requests
      // may arrive and hence make this operation completable.
      delayedFetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys)
    }
  }

  /**
   * Read from multiple topic partitions at the given offset up to maxSize bytes
   */
  //  用给定分区的 offset，从多个主题分区读取消息，直到消息的大小满足设定的最大的值 maxSize。
  def readFromLocalLog(replicaId: Int, // 要读取的replicaId。
                       fetchOnlyFromLeader: Boolean, // 是否只从 leader 分区读取。
                       fetchIsolation: FetchIsolation, // 获取隔离级别
                       fetchMaxBytes: Int, // 获取消息的大小
                       hardMaxBytesLimit: Boolean,
                       readPartitionInfo: Seq[(TopicPartition, PartitionData)], //读哪些分区，从哪个offset开始读取
                       quota: ReplicaQuota,
                       clientMetadata: Option[ClientMetadata]): Seq[(TopicPartition, LogReadResult)] = {
    val traceEnabled = isTraceEnabled

    // 真正负责读取的内部方法，每个分区的读取逻辑, read 方法，核心是调用 partition.readRecords 拉取消息
    // tp  拉取消息TopicPartition信息
    // fetchInfo  拉取消息的元数据信息（与tp相对应）
    // limitBytes 拉取消息的最大值
    // minOneMessage 对能否超过最大字节数做硬限制
    def read(tp: TopicPartition, fetchInfo: PartitionData, limitBytes: Int, minOneMessage: Boolean): LogReadResult = {
      //  获取要获取消息的关键参数
      val offset = fetchInfo.fetchOffset
      // 读取的分区偏移量
      val partitionFetchSize = fetchInfo.maxBytes
      // 该次读取的最大字节数
      val followerLogStartOffset = fetchInfo.logStartOffset
      // follower 的起始偏移量 LSO
      // limitBytes值：Math.min(Math.min(fetchRequest.maxBytes, config.fetchMaxBytes), maxQuotaWindowBytes)
      val adjustedMaxBytes = math.min(fetchInfo.maxBytes, limitBytes)
      // 最大字节数，和传入的最大字节数、该次读取的最大字节数取最小值
      try {
        // 如果日志跟踪可用
        if (traceEnabled)
          trace(s"Fetching log segment for partition $tp, offset $offset, partition fetch size $partitionFetchSize, " +
            s"remaining response limit $limitBytes" +
            // 输出日志
            (if (minOneMessage) s", ignoring response/partition size limits" else ""))

        // 根据 tp 获取 partition 信息即从 broker 的所有分区中找到与传入的分区相匹配的分区
        val partition = getPartitionOrException(tp)
        val fetchTimeMs = time.milliseconds

        // If we are the leader, determine the preferred read-replica
        // 判断哪个副本是首选副本。kafka 从 2.4 版本开始支持从读取效果最好的分区（也包括follower）读取消息，这样能提升读取效率。
        val preferredReadReplica = clientMetadata.flatMap(
          metadata => findPreferredReadReplica(partition, metadata, replicaId, fetchInfo.fetchOffset, fetchTimeMs))

        // 如果已经有首选副本了，则直接返回该副本的信息
        if (preferredReadReplica.isDefined) {
          replicaSelectorOpt.foreach { selector =>
            debug(s"Replica selector ${selector.getClass.getSimpleName} returned preferred replica " +
              s"${preferredReadReplica.get} for $clientMetadata")
          }
          // If a preferred read-replica is set, skip the read
          // 如果有首选副本，则直接返回信息
          val offsetSnapshot = partition.fetchOffsetSnapshot(fetchInfo.currentLeaderEpoch, fetchOnlyFromLeader = false)
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = offsetSnapshot.highWatermark.messageOffset,
            leaderLogStartOffset = offsetSnapshot.logStartOffset,
            leaderLogEndOffset = offsetSnapshot.logEndOffset.messageOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = -1L,
            lastStableOffset = Some(offsetSnapshot.lastStableOffset.messageOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        } else {
          // Try the read first, this tells us whether we need all of adjustedFetchSize for this partition
          // 如果没有首选副本，则从分区中读取本地消息数据。
          val readInfo: LogReadInfo = partition.readRecords(
            lastFetchedEpoch = fetchInfo.lastFetchedEpoch,
            fetchOffset = fetchInfo.fetchOffset,
            currentLeaderEpoch = fetchInfo.currentLeaderEpoch,
            maxBytes = adjustedMaxBytes,
            fetchIsolation = fetchIsolation,
            fetchOnlyFromLeader = fetchOnlyFromLeader,
            minOneMessage = minOneMessage)

          // 根据分区的限制信息，修正读取到的数据
          val fetchDataInfo = if (shouldLeaderThrottle(quota, partition, replicaId)) {
            // If the partition is being throttled, simply return an empty set.
            // 如果分区达到了限制，直接返回空
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else if (!hardMaxBytesLimit && readInfo.fetchedData.firstEntryIncomplete) {
            // For FetchRequest version 3, we replace incomplete message sets with an empty one as consumers can make
            // progress in such cases and don't need to report a `RecordTooLargeException`
            // 如果读取的消息是不完整的，将其设为 empty
            FetchDataInfo(readInfo.fetchedData.fetchOffsetMetadata, MemoryRecords.EMPTY)
          } else {
            readInfo.fetchedData
          }

          // 构建日志读取结构，返回读取结果
          LogReadResult(info = fetchDataInfo,
            divergingEpoch = readInfo.divergingEpoch,
            highWatermark = readInfo.highWatermark,
            leaderLogStartOffset = readInfo.logStartOffset,
            leaderLogEndOffset = readInfo.logEndOffset,
            followerLogStartOffset = followerLogStartOffset,
            fetchTimeMs = fetchTimeMs,
            lastStableOffset = Some(readInfo.lastStableOffset),
            preferredReadReplica = preferredReadReplica,
            exception = None)
        }
      } catch {
        // NOTE: Failed fetch requests metric is not incremented for known exceptions since it
        // is supposed to indicate un-expected failure of a broker in handling a fetch request
        // 处理各种异常
        // 一些异常是预期之内的，不应该计入“失败的抓取请求计数指标”
        // 比如 UnknownTopicOrPartitionException、NotLeaderOrFollowerException、UnknownLeaderEpochException 等
        case e@ (_: UnknownTopicOrPartitionException |
                 _: NotLeaderOrFollowerException |
                 _: UnknownLeaderEpochException |
                 _: FencedLeaderEpochException |
                 _: ReplicaNotAvailableException |
                 _: KafkaStorageException |
                 _: OffsetOutOfRangeException) =>
          // 处理异常时的行为，返回错误信息
          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e))
        case e: Throwable =>
          brokerTopicStats.topicStats(tp.topic).failedFetchRequestRate.mark()
          brokerTopicStats.allTopicsStats.failedFetchRequestRate.mark()

          val fetchSource = Request.describeReplicaId(replicaId)
          error(s"Error processing fetch with max size $adjustedMaxBytes from $fetchSource " +
            s"on partition $tp: $fetchInfo", e)

          LogReadResult(info = FetchDataInfo(LogOffsetMetadata.UnknownOffsetMetadata, MemoryRecords.EMPTY),
            divergingEpoch = None,
            highWatermark = Log.UnknownOffset,
            leaderLogStartOffset = Log.UnknownOffset,
            leaderLogEndOffset = Log.UnknownOffset,
            followerLogStartOffset = Log.UnknownOffset,
            fetchTimeMs = -1L,
            lastStableOffset = None,
            exception = Some(e))
      }
    }

    // 设定当前的最大读取字节数
    var limitBytes = fetchMaxBytes
    // topic-partation类型的数组
    val result = new mutable.ArrayBuffer[(TopicPartition, LogReadResult)]
    // 是否至少读取一个 message，初始化为 true
    var minOneMessage = !hardMaxBytesLimit
    // 在待读取分区上循环调用其日志对象的 read 方法执行实际的消息读取
    // readPartitionInfo 来自消费者，读取分区的信息
    readPartitionInfo.foreach { case (tp, fetchInfo) =>
      // 遍历 TopicPartition，并进行数据读取
      val readResult = read(tp, fetchInfo, limitBytes, minOneMessage)
      // 读取分区数据
      val recordBatchSize = readResult.info.records.sizeInBytes
      // Once we read from a non-empty partition, we stop ignoring request and partition level size limits
      if (recordBatchSize > 0)
      // 如果分区返回了数据，则将 minOneMessage 设为 false，不再忽略限制
        minOneMessage = false
      limitBytes = math.max(0, limitBytes - recordBatchSize)
      result += (tp -> readResult)
    }
    result
  }

  /**
    * Using the configured [[ReplicaSelector]], determine the preferred read replica for a partition given the
    * client metadata, the requested offset, and the current set of replicas. If the preferred read replica is the
    * leader, return None
    */
  def findPreferredReadReplica(partition: Partition,
                               clientMetadata: ClientMetadata,
                               replicaId: Int,
                               fetchOffset: Long,
                               currentTimeMs: Long): Option[Int] = {
    partition.leaderReplicaIdOpt.flatMap { leaderReplicaId =>
      // Don't look up preferred for follower fetches via normal replication
      if (Request.isValidBrokerId(replicaId))
        None
      else {
        replicaSelectorOpt.flatMap { replicaSelector =>
          val replicaEndpoints = metadataCache.getPartitionReplicaEndpoints(partition.topicPartition,
            new ListenerName(clientMetadata.listenerName))
          val replicaInfos = partition.remoteReplicas
            // Exclude replicas that don't have the requested offset (whether or not if they're in the ISR)
            .filter(replica => replica.logEndOffset >= fetchOffset && replica.logStartOffset <= fetchOffset)
            .map(replica => new DefaultReplicaView(
              replicaEndpoints.getOrElse(replica.brokerId, Node.noNode()),
              replica.logEndOffset,
              currentTimeMs - replica.lastCaughtUpTimeMs))

          val leaderReplica = new DefaultReplicaView(
            replicaEndpoints.getOrElse(leaderReplicaId, Node.noNode()),
            partition.localLogOrException.logEndOffset, 0L)
          val replicaInfoSet = mutable.Set[ReplicaView]() ++= replicaInfos += leaderReplica

          val partitionInfo = new DefaultPartitionView(replicaInfoSet.asJava, leaderReplica)
          replicaSelector.select(partition.topicPartition, clientMetadata, partitionInfo).asScala.collect {
            // Even though the replica selector can return the leader, we don't want to send it out with the
            // FetchResponse, so we exclude it here
            case selected if !selected.endpoint.isEmpty && selected != leaderReplica => selected.endpoint.id
          }
        }
      }
    }
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the leader if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  def shouldLeaderThrottle(quota: ReplicaQuota, partition: Partition, replicaId: Int): Boolean = {
    val isReplicaInSync = partition.inSyncReplicaIds.contains(replicaId)
    !isReplicaInSync && quota.isThrottled(partition.topicPartition) && quota.isQuotaExceeded
  }

  def getLogConfig(topicPartition: TopicPartition): Option[LogConfig] = localLog(topicPartition).map(_.config)

  def getMagic(topicPartition: TopicPartition): Option[Byte] = getLogConfig(topicPartition).map(_.messageFormatVersion.recordVersion.value)

  def maybeUpdateMetadataCache(correlationId: Int, updateMetadataRequest: UpdateMetadataRequest) : Seq[TopicPartition] =  {
    replicaStateChangeLock synchronized {
      if(updateMetadataRequest.controllerEpoch < controllerEpoch) {
        val stateControllerEpochErrorMessage = s"Received update metadata request with correlation id $correlationId " +
          s"from an old controller ${updateMetadataRequest.controllerId} with epoch ${updateMetadataRequest.controllerEpoch}. " +
          s"Latest known controller epoch is $controllerEpoch"
        stateChangeLogger.warn(stateControllerEpochErrorMessage)
        throw new ControllerMovedException(stateChangeLogger.messageWithPrefix(stateControllerEpochErrorMessage))
      } else {
        val deletedPartitions = metadataCache.updateMetadata(correlationId, updateMetadataRequest)
        // maybeUpdateMetadataCache方法处理 UpdateMetadataRequest 请求时
        controllerEpoch = updateMetadataRequest.controllerEpoch
        deletedPartitions
      }
    }
  }

  def becomeLeaderOrFollower(correlationId: Int,
                             leaderAndIsrRequest: LeaderAndIsrRequest,
                             onLeadershipChange: (Iterable[Partition], Iterable[Partition]) => Unit): LeaderAndIsrResponse = {
    val startMs = time.milliseconds()
    replicaStateChangeLock synchronized {
      // Controller 所在 Broker 的ID
      val controllerId = leaderAndIsrRequest.controllerId
      val requestPartitionStates = leaderAndIsrRequest.partitionStates.asScala
      stateChangeLogger.info(s"Handling LeaderAndIsr request correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      if (stateChangeLogger.isTraceEnabled)
        requestPartitionStates.foreach { partitionState =>
          stateChangeLogger.trace(s"Received LeaderAndIsr request $partitionState " +
            s"correlation id $correlationId from controller $controllerId " +
            s"epoch ${leaderAndIsrRequest.controllerEpoch}")
        }

      val response = {
        // 如果 LeaderAndIsrRequest 携带的 Controller Epoch 小于当前 Controller Epoch 值
        if (leaderAndIsrRequest.controllerEpoch < controllerEpoch) {
          stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
            s"correlation id $correlationId since its controller epoch ${leaderAndIsrRequest.controllerEpoch} is old. " +
            s"Latest known controller epoch is $controllerEpoch")
          // 表示 Controller 已经易主，抛出相应异常
          leaderAndIsrRequest.getErrorResponse(0, Errors.STALE_CONTROLLER_EPOCH.exception)
        } else {
          val responseMap = new mutable.HashMap[TopicPartition, Errors]
          // becomeLeaderOrFollower方法中处理 LeaderAndIsrRequest 请求时
          controllerEpoch = leaderAndIsrRequest.controllerEpoch

          val partitionStates = new mutable.HashMap[Partition, LeaderAndIsrPartitionState]()

          // First create the partition if it doesn't exist already
          // 遍历 LeaderAndIsrRequest 请求中的所有分区
          requestPartitionStates.foreach { partitionState =>
            val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
            // 从 allPartitions 中获取对应分区对象
            val partitionOpt = getPartition(topicPartition) match {
              case HostedPartition.Offline =>
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition as the local replica for the " +
                  "partition is in an offline log directory")
                responseMap.put(topicPartition, Errors.KAFKA_STORAGE_ERROR)
                None

              // 如果是 Offline 状态
              case HostedPartition.Online(partition) =>
                Some(partition)

              // 如果是 None 状态，则表示没有找到分区对象
              case HostedPartition.None =>
                // 创建新的分区对象
                val partition = Partition(topicPartition, time, this)
                // 新创建的分区对象加入到 allPartitions 统一管理
                allPartitions.putIfNotExists(topicPartition, HostedPartition.Online(partition))
                // 赋值 partitionOpt 字段
                Some(partition)
            }

            // Next check partition's leader epoch
            // 检查分区的 Leader Epoch 值
            partitionOpt.foreach { partition =>
              val currentLeaderEpoch = partition.getLeaderEpoch
              val requestLeaderEpoch = partitionState.leaderEpoch
              if (requestLeaderEpoch > currentLeaderEpoch) {
                // If the leader epoch is valid record the epoch of the controller that made the leadership decision.
                // This is useful while updating the isr to maintain the decision maker controller's epoch in the zookeeper path
                if (partitionState.replicas.contains(localBrokerId))
                  partitionStates.put(partition, partitionState)
                else {
                  stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from controller $controllerId with " +
                    s"correlation id $correlationId epoch $controllerEpoch for partition $topicPartition as itself is not " +
                    s"in assigned replica list ${partitionState.replicas.asScala.mkString(",")}")
                  responseMap.put(topicPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION)
                }
              } else if (requestLeaderEpoch < currentLeaderEpoch) {
                // 如果请求的 Leader Epoch 小于当前的 Leader Epoch，则打印警告日志，并放入错误响应
                stateChangeLogger.warn(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch is smaller than the current " +
                  s"leader epoch $currentLeaderEpoch")
                responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
              } else {
                // 如果请求的 Leader Epoch 等于当前的 Leader Epoch，则打印信息日志，并放入错误响应
                stateChangeLogger.info(s"Ignoring LeaderAndIsr request from " +
                  s"controller $controllerId with correlation id $correlationId " +
                  s"epoch $controllerEpoch for partition $topicPartition since its associated " +
                  s"leader epoch $requestLeaderEpoch matches the current leader epoch")
                responseMap.put(topicPartition, Errors.STALE_CONTROLLER_EPOCH)
              }
            }
          }

          // 确定 Broker 上副本是哪些分区的 Leader 副本
          val partitionsToBeLeader = partitionStates.filter { case (_, partitionState) =>
            partitionState.leader == localBrokerId
          }
          // 确定 Broker 上副本是哪些分区的 Follower 副本
          val partitionsToBeFollower = partitionStates.filter { case (k, _) => !partitionsToBeLeader.contains(k) }

          val highWatermarkCheckpoints = new LazyOffsetCheckpoints(this.highWatermarkCheckpoints)
          // 调用 makeLeaders 方法为 partitionsToBeLeader 所有分区执行【成为Leader副本】的逻辑
          val partitionsBecomeLeader = if (partitionsToBeLeader.nonEmpty)
            makeLeaders(controllerId, controllerEpoch, partitionsToBeLeader, correlationId, responseMap,
              highWatermarkCheckpoints)
          else
            Set.empty[Partition]
          // 调用 makeFollowers 方法为 partitionsToBeFollower 所有分区执行【成为Follower副本】的逻辑
          val partitionsBecomeFollower = if (partitionsToBeFollower.nonEmpty)
            makeFollowers(controllerId, controllerEpoch, partitionsToBeFollower, correlationId, responseMap,
              highWatermarkCheckpoints)
          else
            Set.empty[Partition]

          /*
         * KAFKA-8392
         * For topic partitions of which the broker is no longer a leader, delete metrics related to
         * those topics. Note that this means the broker stops being either a replica or a leader of
         * partitions of said topics
         */
          val leaderTopicSet = leaderPartitionsIterator.map(_.topic).toSet
          val followerTopicSet = partitionsBecomeFollower.map(_.topic).toSet
          followerTopicSet.diff(leaderTopicSet).foreach(brokerTopicStats.removeOldLeaderMetrics)

          // remove metrics for brokers which are not followers of a topic
          leaderTopicSet.diff(followerTopicSet).foreach(brokerTopicStats.removeOldFollowerMetrics)

          leaderAndIsrRequest.partitionStates.forEach { partitionState =>
            val topicPartition = new TopicPartition(partitionState.topicName, partitionState.partitionIndex)
            /*
           * If there is offline log directory, a Partition object may have been created by getOrCreatePartition()
           * before getOrCreateReplica() failed to create local replica due to KafkaStorageException.
           * In this case ReplicaManager.allPartitions will map this topic-partition to an empty Partition object.
           * we need to map this topic-partition to OfflinePartition instead.
           */
            // 如果有分区的本地日志为空，说明底层的日志路径不可用，标记该分区为 Offline 状态
            if (localLog(topicPartition).isEmpty)
              markPartitionOffline(topicPartition)
          }

          // we initialize highwatermark thread after the first leaderisrrequest. This ensures that all the partitions
          // have been completely populated before starting the checkpointing there by avoiding weird race conditions
          // 启动高水位检查点专属线程，定期将 Broker 上所有非 Offline 分区的高水位值写入到检查点文件
          startHighWatermarkCheckPointThread()

          // 添加日志路径数据迁移线程
          maybeAddLogDirFetchers(partitionStates.keySet, highWatermarkCheckpoints)

          // 关闭空闲副本拉取线程
          replicaFetcherManager.shutdownIdleFetcherThreads()
          // 关闭空闲日志路径数据迁移线程
          replicaAlterLogDirsManager.shutdownIdleFetcherThreads()
          // 执行 Leader 变更之后的回调逻辑
          onLeadershipChange(partitionsBecomeLeader, partitionsBecomeFollower)
          val responsePartitions = responseMap.iterator.map { case (tp, error) =>
            new LeaderAndIsrPartitionError()
              .setTopicName(tp.topic)
              .setPartitionIndex(tp.partition)
              .setErrorCode(error.code)
          }.toBuffer
          new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
            .setErrorCode(Errors.NONE.code)
            .setPartitionErrors(responsePartitions.asJava))
        }
      }
      val endMs = time.milliseconds()
      val elapsedMs = endMs - startMs
      stateChangeLogger.info(s"Finished LeaderAndIsr request in ${elapsedMs}ms correlationId $correlationId from controller " +
        s"$controllerId for ${requestPartitionStates.size} partitions")
      response
    }
  }

  private def maybeAddLogDirFetchers(partitions: Set[Partition],
                                     offsetCheckpoints: OffsetCheckpoints): Unit = {
    val futureReplicasAndInitialOffset = new mutable.HashMap[TopicPartition, InitialFetchState]
    for (partition <- partitions) {
      val topicPartition = partition.topicPartition
      if (logManager.getLog(topicPartition, isFuture = true).isDefined) {
        partition.log.foreach { log =>
          val leader = BrokerEndPoint(config.brokerId, "localhost", -1)

          // Add future replica log to partition's map
          partition.createLogIfNotExists(
            isNew = false,
            isFutureReplica = true,
            offsetCheckpoints)

          // pause cleaning for partitions that are being moved and start ReplicaAlterDirThread to move
          // replica from source dir to destination dir
          logManager.abortAndPauseCleaning(topicPartition)

          futureReplicasAndInitialOffset.put(topicPartition, InitialFetchState(leader,
            partition.getLeaderEpoch, log.highWatermark))
        }
      }
    }

    if (futureReplicasAndInitialOffset.nonEmpty)
      replicaAlterLogDirsManager.addFetcherForPartitions(futureReplicasAndInitialOffset)
  }

  /*
   * Make the current broker to become leader for a given set of partitions by:
   *
   * 1. Stop fetchers for these partitions
   * 2. Update the partition metadata in cache
   * 3. Add these partitions to the leader partitions set
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made leader due to this method
   *
   *  TODO: the above may need to be fixed later
   */
  private def makeLeaders(controllerId: Int, // Controller 所在 Broker 的ID,该字段只是用于日志输出，无其他实际用途
                          controllerEpoch: Int, // Controller Epoch 值，它是 Controller 版本号,该字段用于日志输出使用，无其他实际用途。
                          partitionStates: Map[Partition, LeaderAndIsrPartitionState], // LeaderAndIsrRequest请求中携带的分区信息，包括每个分区的Leader是谁、ISR都有哪些等数据。
                          correlationId: Int,  // 请求的 Correlation 字段，只用于日志调试
                          responseMap: mutable.Map[TopicPartition, Errors],  // 按照主题分区分组的异常错误集合
                          highWatermarkCheckpoints: OffsetCheckpoints): Set[Partition] = { // 操作磁盘上高水位检查点文件的工具类
    val traceEnabled = stateChangeLogger.isTraceEnabled
    // 使用 Errors.NONE 初始化 ResponseMap
    partitionStates.keys.foreach { partition =>
      if (traceEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from " +
          s"controller $controllerId epoch $controllerEpoch starting the become-leader transition for " +
          s"partition ${partition.topicPartition}")
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeLeaders = mutable.Set[Partition]()

    try {
      // First stop fetchers for all the partitions
      // 停止消息拉取
      replicaFetcherManager.removeFetcherForPartitions(partitionStates.keySet.map(_.topicPartition))
      stateChangeLogger.info(s"Stopped fetchers as part of LeaderAndIsr request correlationId $correlationId from " +
        s"controller $controllerId epoch $controllerEpoch as part of the become-leader transition for " +
        s"${partitionStates.size} partitions")
      // Update the partition information to be the leader
      // 遍历请求中携带的所有分区
      partitionStates.forKeyValue { (partition, partitionState) =>
        try {
          // 真正执行成为 Leader 的操作
          if (partition.makeLeader(partitionState, highWatermarkCheckpoints))
          // 将分区加入到成为 Leader 副本的分区列表
            partitionsToMakeLeaders += partition
          else
            stateChangeLogger.info(s"Skipped the become-leader state change after marking its " +
              s"partition as leader with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} (last update controller epoch ${partitionState.controllerEpoch}) " +
              s"since it is already the leader for the partition.")
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-leader state change with " +
              s"correlation id $correlationId from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) since " +
              s"the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the leader for partition $partition in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

    } catch {
      case e: Throwable =>
        partitionStates.keys.foreach { partition =>
          stateChangeLogger.error(s"Error while processing LeaderAndIsr request correlationId $correlationId received " +
            s"from controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition}", e)
        }
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    if (traceEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-leader transition for partition ${partition.topicPartition}")
      }

    partitionsToMakeLeaders
  }

  /*
   * Make the current broker to become follower for a given set of partitions by:
   *
   * 1. Remove these partitions from the leader partitions set.
   * 2. Mark the replicas as followers so that no more data can be added from the producer clients.
   * 3. Stop fetchers for these partitions so that no more data can be added by the replica fetcher threads.
   * 4. Truncate the log and checkpoint offsets for these partitions.
   * 5. Clear the produce and fetch requests in the purgatory
   * 6. If the broker is not shutting down, add the fetcher to the new leaders.
   *
   * The ordering of doing these steps make sure that the replicas in transition will not
   * take any more messages before checkpointing offsets so that all messages before the checkpoint
   * are guaranteed to be flushed to disks
   *
   * If an unexpected error is thrown in this function, it will be propagated to KafkaApis where
   * the error message will be set on each partition since we do not know which partition caused it. Otherwise,
   * return the set of partitions that are made follower due to this method
   */
  private def makeFollowers(controllerId: Int, // Controller 所在 Broker 的 Id
                            controllerEpoch: Int, // Controller Epoch值
                            partitionStates: Map[Partition, LeaderAndIsrPartitionState], // 当前 Broker 是Follower 副本的所有分区的详细信息
                            correlationId: Int, // 连接请求与响应的关联字段
                            responseMap: mutable.Map[TopicPartition, Errors], // 封装 LeaderAndIsrRequest 请求处理结果的字段
                            highWatermarkCheckpoints: OffsetCheckpoints) : Set[Partition] = { // 操作高水位检查点文件的工具类
    val traceLoggingEnabled = stateChangeLogger.isTraceEnabled
    // 遍历 partitionStates 所有分区
    partitionStates.forKeyValue { (partition, partitionState) =>
      if (traceLoggingEnabled)
        stateChangeLogger.trace(s"Handling LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch starting the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionState.leader}")
      // 将所有分区的处理结果的状态初始化为 Errors.NONE
      responseMap.put(partition.topicPartition, Errors.NONE)
    }

    val partitionsToMakeFollower: mutable.Set[Partition] = mutable.Set()
    try {
      // TODO: Delete leaders from LeaderAndIsrRequest
      // 遍历 partitionStates 所有分区
      partitionStates.forKeyValue { (partition, partitionState) =>
        // 拿到分区的 Leader Broker ID
        val newLeaderBrokerId = partitionState.leader
        try {
          // 在元数据缓存中找到 Leader Broke 对象
          metadataCache.getAliveBrokers.find(_.id == newLeaderBrokerId) match {
            // Only change partition state when the leader is available
            // 如果 Leader 确实存在
            case Some(_) =>
              // 真正执行成为 Follower 的操作
              if (partition.makeFollower(partitionState, highWatermarkCheckpoints))
              // 将分区加入到成为 Followers 副本的分区列表
                partitionsToMakeFollower += partition
              else
                stateChangeLogger.info(s"Skipped the become-follower state change after marking its partition as " +
                  s"follower with correlation id $correlationId from controller $controllerId epoch $controllerEpoch " +
                  s"for partition ${partition.topicPartition} (last update " +
                  s"controller epoch ${partitionState.controllerEpoch}) " +
                  s"since the new leader $newLeaderBrokerId is the same as the old leader")
            // 如果 Leader 不存在
            case None =>
              // The leader broker should always be present in the metadata cache.
              // If not, we should record the error message and abort the transition process for this partition
              stateChangeLogger.error(s"Received LeaderAndIsrRequest with correlation id $correlationId from " +
                s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
                s"(last update controller epoch ${partitionState.controllerEpoch}) " +
                s"but cannot become follower since the new leader $newLeaderBrokerId is unavailable.")
              // Create the local replica even if the leader is unavailable. This is required to ensure that we include
              // the partition's high watermark in the checkpoint file (see KAFKA-1647)
              // 依然创建出分区 Follower 副本的日志对象
              partition.createLogIfNotExists(isNew = partitionState.isNew, isFutureReplica = false,
                highWatermarkCheckpoints)
          }
        } catch {
          case e: KafkaStorageException =>
            stateChangeLogger.error(s"Skipped the become-follower state change with correlation id $correlationId from " +
              s"controller $controllerId epoch $controllerEpoch for partition ${partition.topicPartition} " +
              s"(last update controller epoch ${partitionState.controllerEpoch}) with leader " +
              s"$newLeaderBrokerId since the replica for the partition is offline due to disk error $e")
            val dirOpt = getLogDir(partition.topicPartition)
            error(s"Error while making broker the follower for partition $partition with leader " +
              s"$newLeaderBrokerId in dir $dirOpt", e)
            responseMap.put(partition.topicPartition, Errors.KAFKA_STORAGE_ERROR)
        }
      }

      // 移除现有 Fetcher 线程
      replicaFetcherManager.removeFetcherForPartitions(partitionsToMakeFollower.map(_.topicPartition))
      stateChangeLogger.info(s"Stopped fetchers as part of become-follower request from controller $controllerId " +
        s"epoch $controllerEpoch with correlation id $correlationId for ${partitionsToMakeFollower.size} partitions")

      // 尝试完成延迟请求
      partitionsToMakeFollower.foreach { partition =>
        completeDelayedFetchOrProduceRequests(partition.topicPartition)
      }

      if (isShuttingDown.get()) {
        if (traceLoggingEnabled) {
          partitionsToMakeFollower.foreach { partition =>
            stateChangeLogger.trace(s"Skipped the adding-fetcher step of the become-follower state " +
              s"change with correlation id $correlationId from controller $controllerId epoch $controllerEpoch for " +
              s"partition ${partition.topicPartition} with leader ${partitionStates(partition).leader} " +
              "since it is shutting down")
          }
        }
      } else {
        // we do not need to check if the leader exists again since this has been done at the beginning of this process
        // 为需要将当前 Broker 设置为 Follower 副本的分区确定 Leader Broker 和起始读取位移值fetchOffset
        val partitionsToMakeFollowerWithLeaderAndOffset = partitionsToMakeFollower.map { partition =>
          val leader = metadataCache.getAliveBrokers.find(_.id == partition.leaderReplicaIdOpt.get).get
            .brokerEndPoint(config.interBrokerListenerName)
          val fetchOffset = partition.localLogOrException.highWatermark
          partition.topicPartition -> InitialFetchState(leader, partition.getLeaderEpoch, fetchOffset)
       }.toMap
        // 使用上一步确定的 Leader Broker 和 fetchOffset 添加新的 Fetcher 线程
        replicaFetcherManager.addFetcherForPartitions(partitionsToMakeFollowerWithLeaderAndOffset)
      }
    } catch {
      case e: Throwable =>
        stateChangeLogger.error(s"Error while processing LeaderAndIsr request with correlationId $correlationId " +
          s"received from controller $controllerId epoch $controllerEpoch", e)
        // Re-throw the exception for it to be caught in KafkaApis
        throw e
    }

    if (traceLoggingEnabled)
      partitionStates.keys.foreach { partition =>
        stateChangeLogger.trace(s"Completed LeaderAndIsr request correlationId $correlationId from controller $controllerId " +
          s"epoch $controllerEpoch for the become-follower transition for partition ${partition.topicPartition} with leader " +
          s"${partitionStates(partition).leader}")
      }

    // 返回需要将当前 Broker 设置为 Follower 副本的分区列表
    partitionsToMakeFollower
  }

  private def maybeShrinkIsr(): Unit = {
    trace("Evaluating ISR list of partitions to see which replicas can be removed from the ISR")

    // Shrink ISRs for non offline partitions
    allPartitions.keys.foreach { topicPartition =>
      nonOfflinePartition(topicPartition).foreach(_.maybeShrinkIsr())
    }
  }

  /**
   * Update the follower's fetch state on the leader based on the last fetch request and update `readResult`.
   * If the follower replica is not recognized to be one of the assigned replicas, do not update
   * `readResult` so that log start/end offset and high watermark is consistent with
   * records in fetch response. Log start/end offset and high watermark may change not only due to
   * this fetch request, e.g., rolling new log segment and removing old log segment may move log
   * start offset further than the last offset in the fetched records. The followers will get the
   * updated leader's state in the next fetch response.
   */
  private def updateFollowerFetchState(followerId: Int,
                                       readResults: Seq[(TopicPartition, LogReadResult)]): Seq[(TopicPartition, LogReadResult)] = {
    // 遍历每个读取结果
    readResults.map { case (topicPartition, readResult) =>
      // 判断是否有异常
      val updatedReadResult = if (readResult.error != Errors.NONE) {
        debug(s"Skipping update of fetch state for follower $followerId since the " +
          s"log read returned error ${readResult.error}")
        // 如果读取结果出错，则跳过更新 follower 的 fetch 状态
        readResult
      } else {
        // 判断此副本是否在在线副本集合里。
        nonOfflinePartition(topicPartition) match {
          //如果在在线副本集合里就更新 Follower 副本拉取状态。
          case Some(partition) =>
            if (partition.updateFollowerFetchState(followerId,
              followerFetchOffsetMetadata = readResult.info.fetchOffsetMetadata,
              followerStartOffset = readResult.followerLogStartOffset,
              followerFetchTimeMs = readResult.fetchTimeMs,
              leaderEndOffset = readResult.leaderLogEndOffset)) {
              readResult
            } else {
              warn(s"Leader $localBrokerId failed to record follower $followerId's position " +
                s"${readResult.info.fetchOffsetMetadata.messageOffset}, and last sent HW since the replica " +
                s"is not recognized to be one of the assigned replicas ${partition.assignmentState.replicas.mkString(",")} " +
                s"for partition $topicPartition. Empty records will be returned for this partition.")
              // 如果更新失败，则记录日志，并返回空的fetch信息
              readResult.withEmptyFetchInfo
            }
          case None =>
            warn(s"While recording the replica LEO, the partition $topicPartition hasn't been created.")
            // 如果分区不存在，则记录日志，并返回原始的读取结果
            readResult
        }
      }
      topicPartition -> updatedReadResult
    }
  }

  private def leaderPartitionsIterator: Iterator[Partition] =
    nonOfflinePartitionsIterator.filter(_.leaderLogIfLocal.isDefined)

  def getLogEndOffset(topicPartition: TopicPartition): Option[Long] =
    nonOfflinePartition(topicPartition).flatMap(_.leaderLogIfLocal.map(_.logEndOffset))

  // Flushes the highwatermark value for all partitions to the highwatermark file
  def checkpointHighWatermarks(): Unit = {
    def putHw(logDirToCheckpoints: mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]],
              log: Log): Unit = {
      val checkpoints = logDirToCheckpoints.getOrElseUpdate(log.parentDir,
        new mutable.AnyRefMap[TopicPartition, Long]())
      checkpoints.put(log.topicPartition, log.highWatermark)
    }

    val logDirToHws = new mutable.AnyRefMap[String, mutable.AnyRefMap[TopicPartition, Long]](
      allPartitions.size)
    nonOfflinePartitionsIterator.foreach { partition =>
      partition.log.foreach(putHw(logDirToHws, _))
      partition.futureLog.foreach(putHw(logDirToHws, _))
    }

    for ((logDir, hws) <- logDirToHws) {
      try highWatermarkCheckpoints.get(logDir).foreach(_.write(hws))
      catch {
        case e: KafkaStorageException =>
          error(s"Error while writing to highwatermark file in directory $logDir", e)
      }
    }
  }

  // Used only by test
  def markPartitionOffline(tp: TopicPartition): Unit = replicaStateChangeLock synchronized {
    allPartitions.put(tp, HostedPartition.Offline)
    Partition.removeMetrics(tp)
  }

  /**
   * The log directory failure handler for the replica
   *
   * @param dir                     the absolute path of the log directory
   * @param sendZkNotification      check if we need to send notification to zookeeper node (needed for unit test)
   */
  def handleLogDirFailure(dir: String, sendZkNotification: Boolean = true): Unit = {
    if (!logManager.isLogDirOnline(dir))
      return
    warn(s"Stopping serving replicas in dir $dir")
    replicaStateChangeLock synchronized {
      val newOfflinePartitions = nonOfflinePartitionsIterator.filter { partition =>
        partition.log.exists { _.parentDir == dir }
      }.map(_.topicPartition).toSet

      val partitionsWithOfflineFutureReplica = nonOfflinePartitionsIterator.filter { partition =>
        partition.futureLog.exists { _.parentDir == dir }
      }.toSet

      replicaFetcherManager.removeFetcherForPartitions(newOfflinePartitions)
      replicaAlterLogDirsManager.removeFetcherForPartitions(newOfflinePartitions ++ partitionsWithOfflineFutureReplica.map(_.topicPartition))

      partitionsWithOfflineFutureReplica.foreach(partition => partition.removeFutureLocalReplica(deleteFromLogDir = false))
      newOfflinePartitions.foreach { topicPartition =>
        markPartitionOffline(topicPartition)
      }
      newOfflinePartitions.map(_.topic).foreach { topic: String =>
        maybeRemoveTopicMetrics(topic)
      }
      highWatermarkCheckpoints = highWatermarkCheckpoints.filter { case (checkpointDir, _) => checkpointDir != dir }

      warn(s"Broker $localBrokerId stopped fetcher for partitions ${newOfflinePartitions.mkString(",")} and stopped moving logs " +
           s"for partitions ${partitionsWithOfflineFutureReplica.mkString(",")} because they are in the failed log directory $dir.")
    }
    logManager.handleLogDirFailure(dir)

    if (sendZkNotification)
      zkClient.propagateLogDirEvent(localBrokerId)
    warn(s"Stopped serving replicas in dir $dir")
  }

  def removeMetrics(): Unit = {
    removeMetric("LeaderCount")
    removeMetric("PartitionCount")
    removeMetric("OfflineReplicaCount")
    removeMetric("UnderReplicatedPartitions")
    removeMetric("UnderMinIsrPartitionCount")
    removeMetric("AtMinIsrPartitionCount")
  }

  // High watermark do not need to be checkpointed only when under unit tests
  def shutdown(checkpointHW: Boolean = true): Unit = {
    info("Shutting down")
    removeMetrics()
    if (logDirFailureHandler != null)
      logDirFailureHandler.shutdown()
    replicaFetcherManager.shutdown()
    replicaAlterLogDirsManager.shutdown()
    delayedFetchPurgatory.shutdown()
    delayedProducePurgatory.shutdown()
    delayedDeleteRecordsPurgatory.shutdown()
    delayedElectLeaderPurgatory.shutdown()
    if (checkpointHW)
      checkpointHighWatermarks()
    replicaSelectorOpt.foreach(_.close)
    info("Shut down completely")
  }

  protected def createReplicaFetcherManager(metrics: Metrics, time: Time, threadNamePrefix: Option[String], quotaManager: ReplicationQuotaManager) = {
    new ReplicaFetcherManager(config, this, metrics, time, threadNamePrefix, quotaManager)
  }

  protected def createReplicaAlterLogDirsManager(quotaManager: ReplicationQuotaManager, brokerTopicStats: BrokerTopicStats) = {
    new ReplicaAlterLogDirsManager(config, this, quotaManager, brokerTopicStats)
  }

  protected def createReplicaSelector(): Option[ReplicaSelector] = {
    config.replicaSelectorClassName.map { className =>
      val tmpReplicaSelector: ReplicaSelector = CoreUtils.createObject[ReplicaSelector](className)
      tmpReplicaSelector.configure(config.originals())
      tmpReplicaSelector
    }
  }

  def lastOffsetForLeaderEpoch(requestedEpochInfo: Map[TopicPartition, OffsetsForLeaderEpochRequest.PartitionData]): Map[TopicPartition, EpochEndOffset] = {
    requestedEpochInfo.map { case (tp, partitionData) =>
      val epochEndOffset = getPartition(tp) match {
        case HostedPartition.Online(partition) =>
          partition.lastOffsetForLeaderEpoch(partitionData.currentLeaderEpoch, partitionData.leaderEpoch,
            fetchOnlyFromLeader = true)

        case HostedPartition.Offline =>
          new EpochEndOffset(Errors.KAFKA_STORAGE_ERROR, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

        case HostedPartition.None if metadataCache.contains(tp) =>
          new EpochEndOffset(Errors.NOT_LEADER_OR_FOLLOWER, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)

        case HostedPartition.None =>
          new EpochEndOffset(Errors.UNKNOWN_TOPIC_OR_PARTITION, UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
      }
      tp -> epochEndOffset
    }
  }

  def electLeaders(
    controller: KafkaController,
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    responseCallback: Map[TopicPartition, ApiError] => Unit,
    requestTimeout: Int
  ): Unit = {

    val deadline = time.milliseconds() + requestTimeout

    def electionCallback(results: Map[TopicPartition, Either[ApiError, Int]]): Unit = {
      val expectedLeaders = mutable.Map.empty[TopicPartition, Int]
      val failures = mutable.Map.empty[TopicPartition, ApiError]
      results.foreach {
        case (partition, Right(leader)) => expectedLeaders += partition -> leader
        case (partition, Left(error)) => failures += partition -> error
      }

      if (expectedLeaders.nonEmpty) {
        val watchKeys = expectedLeaders.iterator.map {
          case (tp, _) => TopicPartitionOperationKey(tp)
        }.toBuffer

        delayedElectLeaderPurgatory.tryCompleteElseWatch(
          new DelayedElectLeader(
            math.max(0, deadline - time.milliseconds()),
            expectedLeaders,
            failures,
            this,
            responseCallback
          ),
          watchKeys
        )
      } else {
          // There are no partitions actually being elected, so return immediately
          responseCallback(failures)
      }
    }

    controller.electLeaders(partitions, electionType, electionCallback)
  }
}
