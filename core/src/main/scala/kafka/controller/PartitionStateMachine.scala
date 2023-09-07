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
import kafka.controller.Election._
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Map, Seq, mutable}

// 一个抽象类，用于管理分区的状态机
abstract class PartitionStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   */
  // 它也是在成功选举为控制器后调用
  def startup(): Unit = {
    info("Initializing partition state")
    // 初始化分区的状态
    initializePartitionState()
    info("Triggering online partition state changes")
    // 触发在线分区状态的改变
    triggerOnlinePartitionStateChange()
    debug(s"Started partition state machine with initial state -> ${controllerContext.partitionStates}")
  }

  /**
   * Invoked on controller shutdown.
   */
  // 在控制器关闭时调用
  def shutdown(): Unit = {
    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   */
  // 在成功 controller 选举和 broker 变更时调用
  def triggerOnlinePartitionStateChange(): Unit = {
    // 从 Controller 元数据缓存中获取处于 NewPartition 或 OfflinePartition 状态的分区集合
    val partitions = controllerContext.partitionsInStates(Set(OfflinePartition, NewPartition))
    // 对处于 NewPartition 或 OfflinePartition 状态的分区触发 OnlinePartition 状态的改变。
    triggerOnlineStateChangeForPartitions(partitions)
  }

  def triggerOnlinePartitionStateChange(topic: String): Unit = {
    // 从 Controller 元数据缓存中获取指定主题中处于 NewPartition 或 OfflinePartition 状态的分区集合
    val partitions = controllerContext.partitionsInStates(topic, Set(OfflinePartition, NewPartition))
    // 对指定主题中处于 NewPartition 或 OfflinePartition 状态的分区触发 OnlinePartition 状态的改变。
    triggerOnlineStateChangeForPartitions(partitions)
  }

  private def triggerOnlineStateChangeForPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
    // that belong to topics to be deleted
    // 尝试将处于 NewPartition 或 OfflinePartition 状态的分区移动到 OnlinePartition 状态，除了属于要删除的主题的分区。
    val partitionsToTrigger = partitions.filter { partition =>
      !controllerContext.isTopicQueuedUpForDeletion(partition.topic)
    }.toSeq

    handleStateChanges(partitionsToTrigger, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   */
  //  在分区状态机启动时调用，为所有现有的分区设置初始状态。
  private def initializePartitionState(): Unit = {
    // 遍历所有主题下的分区数据
    for (topicPartition <- controllerContext.allPartitions) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      // 检查分区的 leader 和 isr 路径是否存在。如果存在，则分区处于 Online 状态
      controllerContext.partitionLeadershipInfo(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          // 检查分区的 Leader 副本是否在线，如果在线则分区处于 Online 状态
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // leader is alive
            controllerContext.putPartitionState(topicPartition, OnlinePartition)
          else
          // 否则处于 Offline 状态。
            controllerContext.putPartitionState(topicPartition, OfflinePartition)
        case None =>
          // 如果在 zookeeper 中找不到分区的信息，则分区处于 NewPartition 状态。
          controllerContext.putPartitionState(topicPartition, NewPartition)
      }
    }
  }

  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    // 处理分区状态的改变，将指定的分区改变为目标状态。
    handleStateChanges(partitions, targetState, None)
  }

  // 处理分区状态的改变，并根据指定的策略进行重新选举 leader。
  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    leaderElectionStrategy: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]]

}

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 */
// partitionState：它是从 Controller 元数据缓存中获取分区状态的方法。
// doHandleStateChanges：它是执行分区状态变更和转换操作的具体方法。接下来会详细剖析其源码。
// initializeLeaderAndIsrForPartitions：它是初始化分区 Leader 和 ISR 副本列表到 ZK 中的方法。
// electLeaderForPartitions：它是为分区选举 Leader 副本的方法。
// doElectLeaderForPartitions：它是批量为分区选举 Leader 副本的方法。
// collectUncleanLeaderElectionState：它是搜集脏选举的状态的方法。
// logInvalidTransition：记录错误之用，其记录一次非法的状态转换。
// logFailedStateChange：它直接调用另外一个logFailedStateChange来记录异常。
// logFailedStateChange：它是直接抛异常错误的方法。
class ZkPartitionStateMachine(config: KafkaConfig,
                              stateChangeLogger: StateChangeLogger,
                              controllerContext: ControllerContext,
                              zkClient: KafkaZkClient,
                              controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends PartitionStateMachine(controllerContext) {

  private val controllerId = config.brokerId
  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * Try to change the state of the given partitions to the given targetState, using the given
   * partitionLeaderElectionStrategyOpt if a leader election is required.
   * @param partitions The partitions
   * @param targetState The state
   * @param partitionLeaderElectionStrategyOpt The leader election strategy if a leader election is required.
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  override def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    if (partitions.nonEmpty) {
      try {
        // 清空 Controller 待发送请求集合，准备本次请求发送
        controllerBrokerRequestBatch.newBatch()
        // 调用 doHandleStateChanges 方法执行真正的状态变更逻辑
        val result = doHandleStateChanges(
          partitions,
          targetState,
          partitionLeaderElectionStrategyOpt
        )
        // Controller 给相关 Broker 发送请求通知状态变化
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        // 返回状态变更处理结果
        result
      } catch {
        // 如果 Controller 已经易主，则记录错误日志，然后重新抛出异常
        // 上层代码会捕获该异常并执行maybeResign方法执行卸任逻辑
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some partitions to $targetState state", e)
          throw e
        case e: Throwable =>
          // 如果是其他异常，记录错误日志，封装错误返回
          error(s"Error while moving some partitions to $targetState state", e)
          partitions.iterator.map(_ -> Left(e)).toMap
      }
    } else {
      // 如果 partitions 为空，直接返回空
      Map.empty
    }
  }

  private def partitionState(partition: TopicPartition): PartitionState = {
    controllerContext.partitionState(partition)
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param partitions  The partitions for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  private def doHandleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState,
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy]
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateChangeLog.isTraceEnabled
    // 初始化新分区的状态为 NonExistentPartition
    partitions.foreach(partition => controllerContext.putPartitionStateIfNotExists(partition, NonExistentPartition))
    // 找出要执行非法状态转换的分区，记录错误日志
    val (validPartitions, invalidPartitions) = controllerContext.checkValidPartitionStateChange(partitions, targetState)
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState))

    // 根据 targetState 进入到不同的 case 分支
    targetState match {
      case NewPartition =>
        // 遍历所有能够执行转换的分区对象
        validPartitions.foreach { partition =>
          stateChangeLog.info(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          // 将该分区对象设置成 NewPartition 状态
          controllerContext.putPartitionState(partition, NewPartition)
        }
        Map.empty
      case OnlinePartition =>
        // 获取未初始化分区列表，也就是NewPartition状态下的所有分区
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)
        // 获取具备 Leader 选举资格的分区列表，只能为 OnlinePartition 和 OfflinePartition 状态的分区选举 Leader
        val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)
        // 初始化 NewPartition 状态分区，在 ZK 中写入 Leader 和 ISR 数据
        if (uninitializedPartitions.nonEmpty) {
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.info(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr}")
            // 循环将该分区对象设置成 OnlinePartition 状态
            controllerContext.putPartitionState(partition, OnlinePartition)
          }
        }
        // 为具备 Leader 选举资格的分区选举 Leader
        if (partitionsToElectLeader.nonEmpty) {
          // 调用 electLeaderForPartitions 进行选举并返回选举结果
          val electionResults = electLeaderForPartitions(
            partitionsToElectLeader,
            partitionLeaderElectionStrategyOpt.getOrElse(
              throw new IllegalArgumentException("Election strategy is a required field when the target state is OnlinePartition")
            )
          )

          // 如果选举结果不为空，则遍历结果
          electionResults.foreach {
            case (partition, Right(leaderAndIsr)) =>
              stateChangeLog.info(
                s"Changed partition $partition from ${partitionState(partition)} to $targetState with state $leaderAndIsr"
              )
              // 将成功选举为 Leader 后的分区设置成 OnlinePartition 状态
              controllerContext.putPartitionState(partition, OnlinePartition)
            case (_, Left(_)) => // Ignore; no need to update partition state on election error
            // 如果选举失败，忽略
          }
          // 返回 Leader 选举结果
          electionResults
        } else {
          Map.empty
        }
      case OfflinePartition =>
        // 遍历所有能够执行转换的分区对象
        validPartitions.foreach { partition =>
          if (traceEnabled)
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          // 将该分区对象设置成目标状态
          controllerContext.putPartitionState(partition, OfflinePartition)
        }
        Map.empty
      case NonExistentPartition =>
        // 遍历所有能够执行转换的分区对象
        validPartitions.foreach { partition =>
          if (traceEnabled)
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          // 将该分区对象设置成目标状态
          controllerContext.putPartitionState(partition, NonExistentPartition)
        }
        Map.empty
    }
  }

  /**
   * Initialize leader and isr partition state in zookeeper.
   * @param partitions The partitions  that we're trying to initialize.
   * @return The partitions that have been successfully initialized.
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    // 获取每个分区的副本列表
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
    // 获取每个分区的所有存活副本
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
        val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
        partition -> liveReplicasForPartition
    }
    // 按照有无存活副本对分区进行分组。
    // 分为两组：有存活副本的分区、无任何存活副本的分区
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }

    partitionsWithoutLiveReplicas.foreach { case (partition, replicas) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${replicas.mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }
    // 为"有存活副本的分区" 选择 Leader 和 ISR
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      // Leader 选择依据：存活副本列表的首个副本被认定为Leader
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
      // ISR 选择依据：存活副本列表被认定为ISR
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap
    val createResponses = try {
      // 将 leader 和 ISR 持久化到 Zookeeper
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs, controllerContext.epochZkVersion)
    } catch {
      case e: ControllerMovedException =>
        error("Controller moved to another broker when trying to create the topic partition state znode", e)
        throw e
      case e: Exception =>
        // 如果持久化过程中出现异常，打印日志，并将有在线副本的分区添加到失败集合
        partitionsWithLiveReplicas.foreach { case (partition, _) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }
    // 处理持久化的结果，并添加请求到 batch 中
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionFullReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    // 返回初始化成功的分区集合
    successfulInitializations
  }

  /**
   * Repeatedly attempt to elect leaders for multiple partitions until there are no more remaining partitions to retry.
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
   *         either the exception that was thrown or new leader & ISR.
   */
  private def electLeaderForPartitions(
    partitions: Seq[TopicPartition],
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    // 初始化剩余的未选举成功的分区列表
    var remaining = partitions
    val finishedElections = mutable.Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]]

    while (remaining.nonEmpty) {
      // 调用 doElectLeaderForPartitions()方法尝试为多个分区选举 Leader，并将返回的结果进行分类。
      val (finished, updatesToRetry) = doElectLeaderForPartitions(remaining, partitionLeaderElectionStrategy)
      // 更新剩余的分区列表为需要重试选举的分区列表
      remaining = updatesToRetry

      // 遍历每个已经完成选举的分区
      finished.foreach {
        // 如果选举失败，将错误 e 记录到日志。
        case (partition, Left(e)) =>
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
        case (_, Right(_)) => // Ignore; success so no need to log failed state change
      }

      // 累计成功选举的分区
      finishedElections ++= finished

      if (remaining.nonEmpty)
        logger.info(s"Retrying leader election with strategy $partitionLeaderElectionStrategy for partitions $remaining")
    }

    // 返回选举结果
    finishedElections.toMap
  }

  /**
   * Try to elect leaders for multiple partitions.
   * Electing a leader for a partition updates partition state in zookeeper.
   *
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A tuple of two values:
   *         1. The partitions and the expected leader and isr that successfully had a leader elected. And exceptions
   *         corresponding to failed elections that should not be retried.
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   */
  private def doElectLeaderForPartitions(
    partitions: Seq[TopicPartition],
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    val getDataResponses = try {
      // 批量获取 ZooKeeper 中给定分区的 znode 节点数据
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }
    // 构建两个容器，分别保存选举失败分区列表和可选举 Leader 分区列表
    val failedElections = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]
    val validLeaderAndIsrs = mutable.Buffer.empty[(TopicPartition, LeaderAndIsr)]

    // 遍历每个分区的 znode 节点数据
    getDataResponses.foreach { getDataResponse =>
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      val currState = partitionState(partition)
      // 如果成功拿到 znode 节点数据
      if (getDataResponse.resultCode == Code.OK) {
        // 解码分区状态节点数据进行匹配
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          // 节点数据中含 Leader 和 ISR 信息
          case Some(leaderIsrAndControllerEpoch) =>
            // 如果节点数据的 Controller Epoch 值大于当前 Controller Epoch 值，
            // 则表示该分区已经被一个更新的 Controller 选举过 Leader 了，此时必须终止本次Leader选举。
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val failMsg = s"Aborted leader election for partition $partition since the LeaderAndIsr path was " +
                s"already written by another controller. This probably means that the current controller $controllerId went through " +
                s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
              // 将该分区加入到选举失败分区列表
              failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
            } else {
              // 将 Leader 和 ISR 信息按照分区分组加入到可选举 Leader 分区列表
              validLeaderAndIsrs += partition -> leaderIsrAndControllerEpoch.leaderAndIsr
            }

          // 如果节点数据不含 Leader 和 ISR 信息
          case None =>
            val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
            // 将该分区加入到选举失败分区列表
            failedElections.put(partition, Left(exception))
        }

        // 如果没有拿到 znode 节点数据，则将该分区加入到选举失败分区列表
      } else if (getDataResponse.resultCode == Code.NONODE) {
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, Left(exception))
      } else {
        failedElections.put(partition, Left(getDataResponse.resultException.get))
      }
    }

    // 判断 validLeaderAndIsrs 容器中是否包含可选举 Leader 的分区。如果一个满足选举 Leader 的分区都没有，方法直接返回
    if (validLeaderAndIsrs.isEmpty) {
      return (failedElections.toMap, Seq.empty)
    }

    // 开始选举 Leader，并根据选举策略将分区进行分类：有 Leader、无 Leader
    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      // 匹配
      case OfflinePartitionLeaderElectionStrategy(allowUnclean) =>
        val partitionsWithUncleanLeaderElectionState = collectUncleanLeaderElectionState(
          validLeaderAndIsrs,
          allowUnclean
        )
        // 为 OfflinePartition 分区选举 Leader
        leaderForOffline(controllerContext, partitionsWithUncleanLeaderElectionState).partition(_.leaderAndIsr.isEmpty)
      case ReassignPartitionLeaderElectionStrategy =>
        // 为副本重分配的分区选举 Leader
        leaderForReassign(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        // 为分区执行 Preferred 副本 Leader 选举
        leaderForPreferredReplica(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        // 为 Broker 正常关闭而受影响的分区选举 Leader
        leaderForControlledShutdown(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
    }
    partitionsWithoutLeaders.foreach { electionResult =>
      // 将所有选举失败的分区全部加入到Leader选举失败分区列表
      val partition = electionResult.topicPartition
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
    }
    // AR 列表
    val recipientsPerPartition = partitionsWithLeaders.map(result => result.topicPartition -> result.liveReplicas).toMap
    // ISR 列表
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map(result => result.topicPartition -> result.leaderAndIsr.get).toMap
    // 使用新选举的 Leader 和 ISR 信息更新 ZooKeeper 上分区的 znode 节点数据
    val UpdateLeaderAndIsrResult(finishedUpdates, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)
    finishedUpdates.forKeyValue { (partition, result) =>
      result.foreach { leaderAndIsr =>
        val replicaAssignment = controllerContext.partitionFullReplicaAssignment(partition)
        // 构建 LeaderAndIsr 请求，并将该请求加入到 Controller 待发送请求集合
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        // 给这些分区所在的 Broker 发送 LeaderAndIsrRequest 请求同步该分区的数据
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipientsPerPartition(partition), partition,
          leaderIsrAndControllerEpoch, replicaAssignment, isNew = false)
      }
    }

    // 返回选举结果，包括成功选举并更新 ZooKeeper 节点的分区、选举失败分区以及ZooKeeper节点更新失败的分区
    (finishedUpdates ++ failedElections, updatesToRetry)
  }

  /* For the provided set of topic partition and partition sync state it attempts to determine if unclean
   * leader election should be performed. Unclean election should be performed if there are no live
   * replica which are in sync and unclean leader election is allowed (allowUnclean parameter is true or
   * the topic has been configured to allow unclean election).
   *
   * @param leaderIsrAndControllerEpochs set of partition to determine if unclean leader election should be
   *                                     allowed
   * @param allowUnclean whether to allow unclean election without having to read the topic configuration
   * @return a sequence of three element tuple:
   *         1. topic partition
   *         2. leader, isr and controller epoc. Some means election should be performed
   *         3. allow unclean
   */
  private def collectUncleanLeaderElectionState(
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
    allowUnclean: Boolean
  ): Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderAndIsrs.partition {
      case (partition, leaderAndIsr) =>
        val liveInSyncReplicas = leaderAndIsr.isr.filter(controllerContext.isReplicaOnline(_, partition))
        liveInSyncReplicas.isEmpty
    }

    val electionForPartitionWithoutLiveReplicas = if (allowUnclean) {
      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), true)
      }
    } else {
      val (logConfigs, failed) = zkClient.getLogConfigs(
        partitionsWithNoLiveInSyncReplicas.iterator.map { case (partition, _) => partition.topic }.toSet,
        config.originals()
      )

      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        if (failed.contains(partition.topic)) {
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
          (partition, None, false)
        } else {
          (
            partition,
            Option(leaderAndIsr),
            logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue()
          )
        }
      }
    }

    electionForPartitionWithoutLiveReplicas ++
    partitionsWithLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
      (partition, Option(leaderAndIsr), false)
    }
  }

  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

object PartitionLeaderElectionAlgorithms {
  def offlinePartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], uncleanLeaderElectionEnabled: Boolean, controllerContext: ControllerContext): Option[Int] = {
    // 从当前分区副本列表中查找首个处于存活状态的 ISR 副本
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      // 如果找不到满足条件的副本，查看是否允许 Unclean Leader 选举，即 Broker 端参数unclean.leader.election.enable 是否等于 true
      if (uncleanLeaderElectionEnabled) {
        // 策略很简单，选择当前 ISR 副本列表中的第一个存活副本作为 Leader
        val leaderOpt = assignment.find(liveReplicas.contains)
        if (leaderOpt.isDefined)
          controllerContext.stats.uncleanLeaderElectionRate.mark()
        leaderOpt
      } else {
        // 如果不允许 Unclean Leader 选举，则返回 None 表示无法选举 Leader
        None
      }
    }
  }

  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    // 在重新分配的分区列表中查找首个处于存活状态的 ISR 副本
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    // 在分区分配列表中查找首个处于存活状态的 ISR 副本
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    // 在分区分配列表中查找首个处于存活状态的 ISR 副本且不在关闭中的副本
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}

// 分区 Leader 选举策略接口
sealed trait PartitionLeaderElectionStrategy

// OfflinePartitionLeaderElectionStrategy： Leader 副本下线而引发的分区 Leader 选举。
// ReassignPartitionLeaderElectionStrategy：执行分区副本重分配操作而引发的分区 Leader 选举。
// PreferredReplicaPartitionLeaderElectionStrategy：执行 Preferred 副本 Leader 选举而引发的分区 Leader 选举。
// ControlledShutdownPartitionLeaderElectionStrategy：正常关闭 Broker 而引发的分区 Leader 选举。

// 离线分区 Leader 选举策略
final case class OfflinePartitionLeaderElectionStrategy(allowUnclean: Boolean) extends PartitionLeaderElectionStrategy
// 分区副本重分配 Leader 选举策略
final case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
// 分区 Preferred 副本 Leader 选举策略
final case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
// Broker Controlled 关闭时 Leader 选举策略
final case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

// ReplicaState接口
sealed trait PartitionState {
  // 定义状态序号
  def state: Byte
  // 定义合法的前置状态
  def validPreviousStates: Set[PartitionState]
}

case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
