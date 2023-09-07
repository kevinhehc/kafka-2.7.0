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
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Seq, mutable}

// 一个抽象类，用于管理副本的状态机
abstract class ReplicaStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  // 它是在成功选举为控制器后调用
  def startup(): Unit = {
    info("Initializing replica state")
    // 初始化副本的状态
    initializeReplicaState()
    info("Triggering online replica state changes")
    // 获取在线和离线副本列表
    val (onlineReplicas, offlineReplicas) = controllerContext.onlineAndOfflineReplicas
    // 处理在线副本的状态变化
    handleStateChanges(onlineReplicas.toSeq, OnlineReplica)
    info("Triggering offline replica state changes")
    // 处理离线副本的状态变化
    handleStateChanges(offlineReplicas.toSeq, OfflineReplica)
    debug(s"Started replica state machine with initial state -> ${controllerContext.replicaStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  // 在控制器关闭时调用
  def shutdown(): Unit = {
    info("Stopped replica state machine")
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  // 在启动副本状态机时调用，用于设置所有现有分区副本的初始状态
  private def initializeReplicaState(): Unit = {
    // 遍历所有分区
    controllerContext.allPartitions.foreach { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      replicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(partition, replicaId)
        if (controllerContext.isReplicaOnline(replicaId, partition)) {
          // 如果副本在线，则将其状态设置为OnlineReplica
          controllerContext.putReplicaState(partitionAndReplica, OnlineReplica)
        } else {
          // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
          // This is required during controller failover since during controller failover a broker can go down,
          // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          // 如果副本所在的broker已宕机，则将其状态设置为 ReplicaDeletionIneligible，用于删除 topic
          controllerContext.putReplicaState(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  // 状态机最重要的处理逻辑方法  子类会实现它
  def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit
}

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous states are
 *                        ReplicaDeletionStarted and OfflineReplica
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */
// NewReplica：副本被刚创建后状态。
// OnlineReplica：副本正常提供服务时状态。
// OfflineReplica：副本服务下线时状态。
// ReplicaDeletionStarted：副本被删除时状态。
// ReplicaDeletionSuccessful：副本被成功删除后状态。
// ReplicaDeletionIneligible：开启副本删除但副本暂时无法被删除时状态。
// NonExistentReplica：副本从副本状态机被移除前所处的状态。
class ZkReplicaStateMachine(config: KafkaConfig,
                            stateChangeLogger: StateChangeLogger,
                            controllerContext: ControllerContext,
                            zkClient: KafkaZkClient,
                            controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends ReplicaStateMachine(controllerContext) with Logging {

  private val controllerId = config.brokerId
  this.logIdent = s"[ReplicaStateMachine controllerId=$controllerId] "

  override def handleStateChanges(replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    if (replicas.nonEmpty) {
      try {
        // 清空 Controller 待发送请求集合
        controllerBrokerRequestBatch.newBatch()
        // 将所有副本对象按照 Broker 进行分组，然后依次执行状态转换操作
        replicas.groupBy(_.replica).forKeyValue { (replicaId, replicas) =>
          doHandleStateChanges(replicaId, replicas, targetState)
        }
        // 发送对应的 Controller 请求给 Broker
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
      } catch {
        // 如果 Controller 已经易主，则记录错误日志然后抛出异常
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some replicas to $targetState state", e)
          throw e
        case e: Throwable => error(s"Error while moving some replicas to $targetState state", e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache
   *
   * @param replicaId The replica for which the state transition is invoked
   * @param replicas The partitions on this replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  private def doHandleStateChanges(replicaId: Int, replicas: Seq[PartitionAndReplica], targetState: ReplicaState): Unit = {
    val stateLogger = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateLogger.isTraceEnabled
    replicas.foreach(replica => controllerContext.putReplicaStateIfNotExists(replica, NonExistentReplica))
    val (validReplicas, invalidReplicas) = controllerContext.checkValidReplicaStateChange(replicas, targetState)
    invalidReplicas.foreach(replica => logInvalidTransition(replica, targetState))

    targetState match {
      case NewReplica =>
        // 遍历所有能够执行转换的副本对象
        validReplicas.foreach { replica =>
          // 获取该副本对象的分区对象，即 <主题名，分区号> 数据
          val partition = replica.topicPartition
          // 从 Controller 元数据中获取副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)

          // 尝试从 Controller 元数据缓存中获取该分区当前信息, 包括 Leader、ISR 副本列表等信息
          controllerContext.partitionLeadershipInfo(partition) match {
            // 如果成功拿到分区数据信息
            case Some(leaderIsrAndControllerEpoch) =>
              // 如果该副本是 Leader 副本
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId) {
                val exception = new StateChangeFailedException(s"Replica $replicaId for partition $partition cannot be moved to NewReplica state as it is being requested to become leader")
                // 记录错误日志，因为 Leader 副本不能被设置成 NewReplica 状态。
                logFailedStateChange(replica, currentState, OfflineReplica, exception)
              } else {
                // 如果该副本不是 Leader 副本，则给该副本所在的 Broker 发送 LeaderAndIsrRequest 请求同步该分区的数据,
                // 之后再给集群当前所有 Broker 发送 UpdateMetadataRequest 通知它们该分区数据发生变更。
                controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                  replica.topicPartition,
                  leaderIsrAndControllerEpoch,
                  controllerContext.partitionFullReplicaAssignment(replica.topicPartition),
                  isNew = true)
                if (traceEnabled)
                  logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
                // 接着更新元数据缓存中该副本对象的当前状态为 NewReplica
                controllerContext.putReplicaState(replica, NewReplica)
              }
            case None =>
              // 如果没有相应数据
              if (traceEnabled)
                logSuccessfulTransition(stateLogger, replicaId, partition, currentState, NewReplica)
              // 仅仅更新元数据缓存中该副本对象的当前状态为 NewReplica
              controllerContext.putReplicaState(replica, NewReplica)
          }
        }
      case OnlineReplica =>
        // 遍历所有能够执行转换的副本对象
        validReplicas.foreach { replica =>
          // 获取该副本对象的分区对象，即 <主题名，分区号> 数据
          val partition = replica.topicPartition
          // 从 Controller 元数据中获取副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)

          currentState match {
            // 如果当前状态是 NewReplica
            case NewReplica =>
              // 从元数据缓存中拿到分区副本列表
              val assignment = controllerContext.partitionFullReplicaAssignment(partition)
              // 如果副本列表不包含当前副本，则为异常情况
              if (!assignment.replicas.contains(replicaId)) {
                error(s"Adding replica ($replicaId) that is not part of the assignment $assignment")
                // 将该副本加入到副本列表中，并更新元数据缓存中该分区的副本列表
                val newAssignment = assignment.copy(replicas = assignment.replicas :+ replicaId)
                controllerContext.updatePartitionFullReplicaAssignment(partition, newAssignment)
              }
            // 如果当前状态是其他状态
            case _ =>
              // 尝试获取该分区当前信息数据
              controllerContext.partitionLeadershipInfo(partition) match {
                // 如果存在分区信息则向该副本对象所在 Broker 发送请求同步该分区数据
                case Some(leaderIsrAndControllerEpoch) =>
                  controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(Seq(replicaId),
                    replica.topicPartition,
                    leaderIsrAndControllerEpoch,
                    controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
                case None =>
              }
          }
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OnlineReplica)
          // 将该副本对象设置成 OnlineReplica 状态
          controllerContext.putReplicaState(replica, OnlineReplica)
        }
      case OfflineReplica =>
        // 遍历所有能够执行转换的副本对象
        validReplicas.foreach { replica =>
          // 向副本所在 Broker 发送 StopReplicaRequest 请求，停止对应副本。
          // StopReplicaRequest 请求被发送出去之后，这些 Broker 上对应的副本就停止工作了。
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = false)
        }
        // 将副本对象集合划分成有 Leader 信息的副本集合和无 Leader 信息的副本集合
        // 有无 Leader 信息并不仅仅包含 Leader，还有 ISR 和 controllerEpoch 等数据
        val (replicasWithLeadershipInfo, replicasWithoutLeadershipInfo) = validReplicas.partition { replica =>
          controllerContext.partitionLeadershipInfo(replica.topicPartition).isDefined
        }
        // 对于有 Leader 信息的副本集合来说，从它们对应的所有分区中移除该副本对象并更新 ZooKeeper节点
        val updatedLeaderIsrAndControllerEpochs = removeReplicasFromIsr(replicaId, replicasWithLeadershipInfo.map(_.topicPartition))
        // 遍历每个更新过的分区信息
        updatedLeaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
          stateLogger.info(s"Partition $partition state changed to $leaderIsrAndControllerEpoch after removing replica $replicaId from the ISR as part of transition to $OfflineReplica")
          // 如果分区对应主题并未被删除
          if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
            // 获取该分区除给定副本之外的其他副本所在的 Broker
            val recipients = controllerContext.partitionReplicaAssignment(partition).filterNot(_ == replicaId)
            // 向这些 Broker 发送 LeaderAndIsrRequest 请求，去更新停止副本操作之后的分区信息
            controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipients,
              partition,
              leaderIsrAndControllerEpoch,
              controllerContext.partitionFullReplicaAssignment(partition), isNew = false)
          }
          // 获取分区的副本列表
          val replica = PartitionAndReplica(partition, replicaId)
          // 从 Controller 元数据中获取副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, partition, currentState, OfflineReplica)
          // 设置该分区给定副本的状态为 OfflineReplica
          controllerContext.putReplicaState(replica, OfflineReplica)
        }

        // 接着遍历无 Leader 信息的所有副本对象
        replicasWithoutLeadershipInfo.foreach { replica =>
          // 从 Controller 元数据中获取无 Leader 副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, OfflineReplica)
          // 向集群所有 Broker 发送请求，更新对应分区的元数据
          // 对无 Leader 来说，因为我们没有执行任何 Leader 选举操作，所以给这些副本所在的 Broker 发送的就不是
          // LeaderAndIsrRequest 请求了，而是 UpdateMetadataRequest 请求，去告知它们更新对应分区的元数据。
          controllerBrokerRequestBatch.addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(replica.topicPartition))
          // 设置该分区给定副本的状态为 OfflineReplica
          controllerContext.putReplicaState(replica, OfflineReplica)
        }
      case ReplicaDeletionStarted =>
        // 遍历所有能够执行转换的副本对象
        validReplicas.foreach { replica =>
          // 从 Controller 元数据中获取副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionStarted)
          // 设置该分区给定副本的状态为 ReplicaDeletionStarted
          controllerContext.putReplicaState(replica, ReplicaDeletionStarted)
          // 向副本所在 Broker 发送 StopReplicaRequest 请求，停止对应副本。
          controllerBrokerRequestBatch.addStopReplicaRequestForBrokers(Seq(replicaId), replica.topicPartition, deletePartition = true)
        }
      case ReplicaDeletionIneligible =>
        // 遍历所有能够执行转换的副本对象
        validReplicas.foreach { replica =>
          // 从 Controller 元数据中获取副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionIneligible)
          // 因为暂时无法删除，所以直接设置该分区给定副本的状态为 ReplicaDeletionIneligible
          controllerContext.putReplicaState(replica, ReplicaDeletionIneligible)
        }
      case ReplicaDeletionSuccessful =>
        // 遍历所有能够执行转换的副本对象
        validReplicas.foreach { replica =>
          // 从 Controller 元数据中获取副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, ReplicaDeletionSuccessful)
          // 因为删除成功了，所以直接设置该分区给定副本的状态为 ReplicaDeletionSuccessful
          controllerContext.putReplicaState(replica, ReplicaDeletionSuccessful)
        }
      case NonExistentReplica =>
        // 遍历所有能够执行转换的副本对象
        validReplicas.foreach { replica =>
          // 从 Controller 元数据中获取副本对象的当前状态
          val currentState = controllerContext.replicaState(replica)
          // 从分区的完整副本分配中移除当前副本。
          val newAssignedReplicas = controllerContext
            .partitionFullReplicaAssignment(replica.topicPartition)
            .removeReplica(replica.replica)

          // 更新分区的完整副本分配。
          controllerContext.updatePartitionFullReplicaAssignment(replica.topicPartition, newAssignedReplicas)
          if (traceEnabled)
            logSuccessfulTransition(stateLogger, replicaId, replica.topicPartition, currentState, NonExistentReplica)
          // 从副本状态机将移除该副本数据
          controllerContext.removeReplicaState(replica)
        }
    }
  }

  /**
   * Repeatedly attempt to remove a replica from the isr of multiple partitions until there are no more remaining partitions
   * to retry.
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return The updated LeaderIsrAndControllerEpochs of all partitions for which we successfully removed the replica from isr.
   */
  // 从 ISR 副本列表中移除副本
  private def removeReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): Map[TopicPartition, LeaderIsrAndControllerEpoch] = {
    // 用于存储处理后的结果，初始值为空的Map。
    var results = Map.empty[TopicPartition, LeaderIsrAndControllerEpoch]
    // 初始化剩余的分区列表
    var remaining = partitions
    while (remaining.nonEmpty) {
      // 调用 doRemoveReplicasFromIsr()方法执行副本从 ISR 列表中移除的操作，并将返回的结果进行分类
      val (finishedRemoval, removalsToRetry) = doRemoveReplicasFromIsr(replicaId, remaining)
      // 更新剩余的分区列表为需要重试的分区列表
      remaining = removalsToRetry

      // 遍历每个已经完成移除的分区
      finishedRemoval.foreach {
        case (partition, Left(e)) =>
          // 如果移除失败，将错误 e 记录到日志。
            val replica = PartitionAndReplica(partition, replicaId)
            val currentState = controllerContext.replicaState(replica)
            logFailedStateChange(replica, currentState, OfflineReplica, e)
        case (partition, Right(leaderIsrAndEpoch)) =>
          // 如果移除成功，将分区和对应的 LeaderIsrAndControllerEpoch 添加到 results 中。
          results += partition -> leaderIsrAndEpoch
      }
    }
    results
  }

  /**
   * Try to remove a replica from the isr of multiple partitions.
   * Removing a replica from isr updates partition state in zookeeper.
   *
   * @param replicaId The replica being removed from isr of multiple partitions
   * @param partitions The partitions from which we're trying to remove the replica from isr
   * @return A tuple of two elements:
   *         1. The updated Right[LeaderIsrAndControllerEpochs] of all partitions for which we successfully
   *         removed the replica from isr. Or Left[Exception] corresponding to failed removals that should
   *         not be retried
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   */
  // 从 ISR 集合中移除指定副本并更新 Zookeeper 中的 Leader 和 isr信息
  private def doRemoveReplicasFromIsr(
    replicaId: Int,
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]], Seq[TopicPartition]) = {
    // 从 Zookeeper 中获取待更新的 TopicPartition 的 Leader 和 Isr 信息，以及不存在 Leader 和 Isr信息的 TopicPartition 集合
    val (leaderAndIsrs, partitionsWithNoLeaderAndIsrInZk) = getTopicPartitionStatesFromZk(partitions)
    // 将 Leader 和 Isr 信息中包含 replicaId 的 TopicPartition 分为包含和不包含该副本的两个集合
    val (leaderAndIsrsWithReplica, leaderAndIsrsWithoutReplica) = leaderAndIsrs.partition { case (_, result) =>
      result.map { leaderAndIsr =>
        leaderAndIsr.isr.contains(replicaId)
      }.getOrElse(false)
    }

    // 调整 Leader 和 Isr 信息
    val adjustedLeaderAndIsrs: Map[TopicPartition, LeaderAndIsr] = leaderAndIsrsWithReplica.flatMap {
      case (partition, result) =>
        result.toOption.map { leaderAndIsr =>
          // 计算新 Leader，当前要离线的副本 == Leader, 那么这个新Leader == -1；否则新 Leader 还是等于原 Leader。
          val newLeader = if (replicaId == leaderAndIsr.leader) LeaderAndIsr.NoLeader else leaderAndIsr.leader
          // 当 Isr 副本集合的数量只剩下 1 个的时候,那么 ISR 等于原 ISR,否则新 ISR =（原ISR - 当前的被离线的副本）
          val adjustedIsr = if (leaderAndIsr.isr.size == 1) leaderAndIsr.isr else leaderAndIsr.isr.filter(_ != replicaId)
          partition -> leaderAndIsr.newLeaderAndIsr(newLeader, adjustedIsr)
        }
    }

    // 将调整后的 Leader 和 Isr 信息更新到 Zookeeper 中
    val UpdateLeaderAndIsrResult(finishedPartitions, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

    // 处理没有 Leader和 Isr 信息的 TopicPartition，如果该 TopicPartition 不在待删除队列中，则返回一个异常信息
    val exceptionsForPartitionsWithNoLeaderAndIsrInZk: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      partitionsWithNoLeaderAndIsrInZk.iterator.flatMap { partition =>
        if (!controllerContext.isTopicQueuedUpForDeletion(partition.topic)) {
          val exception = new StateChangeFailedException(
            s"Failed to change state of replica $replicaId for partition $partition since the leader and isr " +
            "path in zookeeper is empty"
          )
          Option(partition -> Left(exception))
        } else None
      }.toMap

    // 处理已完成的 TopicPartition，将其对应的 LeaderIsrAndControllerEpoch 信息更新到 Controller 元数据缓存中
    val leaderIsrAndControllerEpochs: Map[TopicPartition, Either[Exception, LeaderIsrAndControllerEpoch]] =
      (leaderAndIsrsWithoutReplica ++ finishedPartitions).map { case (partition, result) =>
        (partition, result.map { leaderAndIsr =>
          val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
          controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
          leaderIsrAndControllerEpoch
        })
      }

    // 返回更新后的信息
    (leaderIsrAndControllerEpochs ++ exceptionsForPartitionsWithNoLeaderAndIsrInZk, updatesToRetry)
  }

  /**
   * Gets the partition state from zookeeper
   * @param partitions the partitions whose state we want from zookeeper
   * @return A tuple of two values:
   *         1. The Right(LeaderAndIsrs) of partitions whose state we successfully read from zookeeper.
   *         The Left(Exception) to failed zookeeper lookups or states whose controller epoch exceeds our current epoch
   *         2. The partitions that had no leader and isr state in zookeeper. This happens if the controller
   *         didn't finish partition initialization.
   */
  // 从 ZooKeeper 获取指定分区的主题和分区的状态
  private def getTopicPartitionStatesFromZk(
    partitions: Seq[TopicPartition]
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    // 尝试从 ZooKeeper 获取分区的状态数据
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      // 如果发生异常，返回所有分区的异常状态，并且没有分区数据
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }

    // 保存没有在 ZooKeeper 中找到 Leader 和 ISR 的分区
    val partitionsWithNoLeaderAndIsrInZk = mutable.Buffer.empty[TopicPartition]
    // 保存每个分区的状态结果
    val result = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]

    // 遍历从 ZooKeeper 中获取分区的状态数据结果
    getDataResponses.foreach[Unit] { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      // 如果获取成功
      if (getDataResponse.resultCode == Code.OK) {
        // 解码获取的状态数据
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          // 如果无法解码，说明该分区在 ZooKeeper 中没有 Leader 和 ISR 数据
          case None =>
            partitionsWithNoLeaderAndIsrInZk += partition
          case Some(leaderIsrAndControllerEpoch) =>
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              // 如果获取到的 ControllerEpoch 大于当前 ControllerEpoch，则说明该分区的领导者和 ISR 数据可能被其他控制器写入，
              // 这可能意味着当前控制器发生了软故障，另一个具有更高 ControllerEpoch 的控制器被选举为新的控制器，所以中止该控制器的状态更改
              val exception = new StateChangeFailedException(
                "Leader and isr path written by another controller. This probably " +
                s"means the current controller with epoch ${controllerContext.epoch} went through a soft failure and " +
                s"another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}. Aborting " +
                "state change by this controller"
              )
              result += (partition -> Left(exception))
            } else {
              // 否则，将获取到的 Leader 和 ISR 数据保存到结果中
              result += (partition -> Right(leaderIsrAndControllerEpoch.leaderAndIsr))
            }
        }
      } else if (getDataResponse.resultCode == Code.NONODE) {
        // 如果结果是 NONODE，说明该分区在 ZooKeeper 中不存在
        partitionsWithNoLeaderAndIsrInZk += partition
      } else {
        // / 其他情况下，将分区的状态设置为异常状态，并保存到结果中
        result += (partition -> Left(getDataResponse.resultException.get))
      }
    }

    // 返回结果
    (result.toMap, partitionsWithNoLeaderAndIsrInZk)
  }

  private def logSuccessfulTransition(logger: StateChangeLogger, replicaId: Int, partition: TopicPartition,
                                      currState: ReplicaState, targetState: ReplicaState): Unit = {
    logger.trace(s"Changed state of replica $replicaId for partition $partition from $currState to $targetState")
  }

  private def logInvalidTransition(replica: PartitionAndReplica, targetState: ReplicaState): Unit = {
    val currState = controllerContext.replicaState(replica)
    val e = new IllegalStateException(s"Replica $replica should be in the ${targetState.validPreviousStates.mkString(",")} " +
      s"states before moving to $targetState state. Instead it is in $currState state")
    logFailedStateChange(replica, currState, targetState, e)
  }

  private def logFailedStateChange(replica: PartitionAndReplica, currState: ReplicaState, targetState: ReplicaState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} initiated state change of replica ${replica.replica} " +
        s"for partition ${replica.topicPartition} from $currState to $targetState failed", t)
  }
}

// ReplicaState接口
sealed trait ReplicaState {
  // 定义状态序号
  def state: Byte
  // 定义合法的前置状态
  def validPreviousStates: Set[ReplicaState]
}

case object NewReplica extends ReplicaState {
  val state: Byte = 1
  val validPreviousStates: Set[ReplicaState] = Set(NonExistentReplica)
}

case object OnlineReplica extends ReplicaState {
  val state: Byte = 2
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object OfflineReplica extends ReplicaState {
  val state: Byte = 3
  val validPreviousStates: Set[ReplicaState] = Set(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible)
}

case object ReplicaDeletionStarted extends ReplicaState {
  val state: Byte = 4
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica)
}

case object ReplicaDeletionSuccessful extends ReplicaState {
  val state: Byte = 5
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionStarted)
}

case object ReplicaDeletionIneligible extends ReplicaState {
  val state: Byte = 6
  val validPreviousStates: Set[ReplicaState] = Set(OfflineReplica, ReplicaDeletionStarted)
}

case object NonExistentReplica extends ReplicaState {
  val state: Byte = 7
  val validPreviousStates: Set[ReplicaState] = Set(ReplicaDeletionSuccessful)
}
