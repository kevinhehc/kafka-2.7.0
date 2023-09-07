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

import java.nio.ByteBuffer
import java.util
import java.util.Optional
import java.util.concurrent.locks.ReentrantLock

import kafka.cluster.BrokerEndPoint
import kafka.utils.{DelayedItem, Pool, ShutdownableThread}
import kafka.utils.Implicits._
import org.apache.kafka.common.errors._
import org.apache.kafka.common.requests.EpochEndOffset._
import kafka.common.ClientIdAndBroker
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import org.apache.kafka.common.protocol.Errors

import scala.collection.{mutable, Map, Set}
import scala.jdk.CollectionConverters._
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import kafka.log.LogAppendInfo
import kafka.server.AbstractFetcherThread.ReplicaFetch
import kafka.server.AbstractFetcherThread.ResultWithPartitions
import org.apache.kafka.common.{InvalidRecordException, TopicPartition}
import org.apache.kafka.common.internals.PartitionStates
import org.apache.kafka.common.record.{FileRecords, MemoryRecords, Records}
import org.apache.kafka.common.requests._

import scala.math._

/**
 *  Abstract class for fetching data from multiple partitions from the same broker.
 */
abstract class AbstractFetcherThread(name: String, // 线程名称
                                     clientId: String, // Client Id，用于日志输出
                                     val sourceBroker: BrokerEndPoint, // 数据源 Broker 地址
                                     failedPartitions: FailedPartitions, // 处理过程中出现失败的分区
                                     fetchBackOffMs: Int = 0, // 获取操作重试间隔
                                     isInterruptible: Boolean = true,  // 线程是否允许被中断

                                     // Broker端主题监控指标
                                     val brokerTopicStats: BrokerTopicStats) //BrokerTopicStats's lifecycle managed by ReplicaManager

  extends ShutdownableThread(name, isInterruptible) {

  // 获取的消息数据
  type FetchData = FetchResponse.PartitionData[Records]
  // Leader Epoch 数据
  type EpochData = OffsetsForLeaderEpochRequest.PartitionData

  // 分区状态
  private val partitionStates = new PartitionStates[PartitionFetchState]
  // 分区 ReentrantLock
  protected val partitionMapLock = new ReentrantLock
  private val partitionMapCond = partitionMapLock.newCondition()

  private val metricId = ClientIdAndBroker(clientId, sourceBroker.host, sourceBroker.port)
  val fetcherStats = new FetcherStats(metricId)
  val fetcherLagStats = new FetcherLagStats(metricId)

  /* callbacks to be defined in subclass */

  // process fetched data
  protected def processPartitionData(topicPartition: TopicPartition, // 读取哪个分区的数据
                                     fetchOffset: Long, // 读取到的最新位移值

                                     // 读取到的分区消息数据
                                     // 写入已读取消息数据前的元数据
                                     partitionData: FetchData): Option[LogAppendInfo]

  // 要对哪个分区下副本执行截断操作
  // Offset + 截断状态
  protected def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit

  protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit

  // 一组要读取的分区列表，分区是否可读取取决于 PartitionFetchState 中的状态
  // 用来封装 FetchRequest.Builder 对象
  protected def buildFetch(partitionMap: Map[TopicPartition, PartitionFetchState]): ResultWithPartitions[Option[ReplicaFetch]]

  protected def latestEpoch(topicPartition: TopicPartition): Option[Int]

  protected def logStartOffset(topicPartition: TopicPartition): Long

  protected def logEndOffset(topicPartition: TopicPartition): Long

  protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Option[OffsetAndEpoch]

  protected def fetchEpochEndOffsets(partitions: Map[TopicPartition, EpochData]): Map[TopicPartition, EpochEndOffset]

  protected def fetchFromLeader(fetchRequest: FetchRequest.Builder): Map[TopicPartition, FetchData]

  protected def fetchEarliestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long

  protected def fetchLatestOffsetFromLeader(topicPartition: TopicPartition, currentLeaderEpoch: Int): Long

  protected def isOffsetForLeaderEpochSupported: Boolean

  override def shutdown(): Unit = {
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }

  override def doWork(): Unit = {
    // 执行副本截断操作
    maybeTruncate()
    // 执行消息获取操作
    maybeFetch()
  }

  private def maybeFetch(): Unit = {
    val fetchRequestOpt = inLock(partitionMapLock) {
      // 为 partitionStates 中的分区构造 FetchRequest, partitionStates 中保存的是要去获取消息的分区以及对应的状态
      val ResultWithPartitions(fetchRequestOpt, partitionsWithError) = buildFetch(partitionStates.partitionStateMap.asScala)

      // 处理出错的分区，处理方式主要是将这个分区加入到有序 LinkedHashMap 末尾等待后续重试
      handlePartitionsWithErrors(partitionsWithError, "maybeFetch")

      // 如果当前没有可读取的分区，则等待 fetchBackOffMs 时间等候后续重试
      if (fetchRequestOpt.isEmpty) {
        trace(s"There are no active partitions. Back off for $fetchBackOffMs ms before sending a fetch request")
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }

      fetchRequestOpt
    }

    // 发送 FETCH 请求给 Leader 副本，并处理 Response
    fetchRequestOpt.foreach { case ReplicaFetch(sessionPartitions, fetchRequest) =>
      processFetchRequest(sessionPartitions, fetchRequest)
    }
  }

  // deal with partitions with errors, potentially due to leadership changes
  private def handlePartitionsWithErrors(partitions: Iterable[TopicPartition], methodName: String): Unit = {
    if (partitions.nonEmpty) {
      debug(s"Handling errors in $methodName for partitions $partitions")
      // 延迟重试处理分区
      delayPartitions(partitions, fetchBackOffMs)
    }
  }

  /**
   * Builds offset for leader epoch requests for partitions that are in the truncating phase based
   * on latest epochs of the future replicas (the one that is fetching)
   */
  private def fetchTruncatingPartitions(): (Map[TopicPartition, EpochData], Set[TopicPartition]) = inLock(partitionMapLock) {
    val partitionsWithEpochs = mutable.Map.empty[TopicPartition, EpochData]
    val partitionsWithoutEpochs = mutable.Set.empty[TopicPartition]

    partitionStates.partitionStateMap.forEach { (tp, state) =>
      if (state.isTruncating) {
        latestEpoch(tp) match {
          case Some(epoch) if isOffsetForLeaderEpochSupported =>
            partitionsWithEpochs += tp -> new EpochData(Optional.of(state.currentLeaderEpoch), epoch)
          case _ =>
            partitionsWithoutEpochs += tp
        }
      }
    }

    (partitionsWithEpochs, partitionsWithoutEpochs)
  }

  private def maybeTruncate(): Unit = {
    // 将所有处于截断中状态的分区依据有无 Leader Epoch 值进行分组
    val (partitionsWithEpochs, partitionsWithoutEpochs) = fetchTruncatingPartitions()
    // 对于有 Leader Epoch 值的分区，将日志截断到 Leader Epoch 值对应的位移值处
    if (partitionsWithEpochs.nonEmpty) {
      truncateToEpochEndOffsets(partitionsWithEpochs)
    }
    // 对于没有 Leader Epoch 值的分区，将日志截断到高水位值处
    if (partitionsWithoutEpochs.nonEmpty) {
      truncateToHighWatermark(partitionsWithoutEpochs)
    }
  }

  private def doTruncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Boolean = {
    try {
      truncate(topicPartition, truncationState)
      true
    }
    catch {
      case e: KafkaStorageException =>
        error(s"Failed to truncate $topicPartition at offset ${truncationState.offset}", e)
        markPartitionFailed(topicPartition)
        false
      case t: Throwable =>
        error(s"Unexpected error occurred during truncation for $topicPartition "
          + s"at offset ${truncationState.offset}", t)
        markPartitionFailed(topicPartition)
        false
    }
  }

  /**
    * - Build a leader epoch fetch based on partitions that are in the Truncating phase
    * - Send OffsetsForLeaderEpochRequest, retrieving the latest offset for each partition's
    *   leader epoch. This is the offset the follower should truncate to ensure
    *   accurate log replication.
    * - Finally truncate the logs for partitions in the truncating phase and mark the
    *   truncation complete. Do this within a lock to ensure no leadership changes can
    *   occur during truncation.
    */
  // 根据指定 TopicPartition 的 PartitionData，获取每个分区的最新的 offset 信息，然后对所有分区的 Log 进行截断操作。
  private def truncateToEpochEndOffsets(latestEpochsForPartitions: Map[TopicPartition, EpochData]): Unit = {
    // 获取每个分区的最新 offset 信息
    val endOffsets = fetchEpochEndOffsets(latestEpochsForPartitions)
    //Ensure we hold a lock during truncation.
    // 加锁，保证截断操作期间不会受到其他线程的干扰
    inLock(partitionMapLock) {
      //Check no leadership and no leader epoch changes happened whilst we were unlocked, fetching epochs
      // 检查在获取 epoch 时，没有发生 Leader 更改
      val epochEndOffsets = endOffsets.filter { case (tp, _) =>
        val curPartitionState = partitionStates.stateValue(tp)
        val partitionEpochRequest = latestEpochsForPartitions.getOrElse(tp, {
          throw new IllegalStateException(
            s"Leader replied with partition $tp not requested in OffsetsForLeaderEpoch request")
        })
        val leaderEpochInRequest = partitionEpochRequest.currentLeaderEpoch.get
        curPartitionState != null && leaderEpochInRequest == curPartitionState.currentLeaderEpoch
      }

      // 对指定 TopicPartition 进行截断操作
      val ResultWithPartitions(fetchOffsets, partitionsWithError) = maybeTruncateToEpochEndOffsets(epochEndOffsets, latestEpochsForPartitions)
      // 处理截断操作中出现的错误
      handlePartitionsWithErrors(partitionsWithError, "truncateToEpochEndOffsets")
      // 更新 fetchOffset 信息，并在可能的情况下标记截断操作已完成
      updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets)
    }
  }

  // Visible for testing
  private[server] def truncateToHighWatermark(partitions: Set[TopicPartition]): Unit = inLock(partitionMapLock) {
    val fetchOffsets = mutable.HashMap.empty[TopicPartition, OffsetTruncationState]

    // 遍历每个要执行截断操作的分区对象
    for (tp <- partitions) {
      // 获取分区的分区读取状态
      val partitionState = partitionStates.stateValue(tp)
      if (partitionState != null) {
        // 取出高水位值，分区的最大可读取位移值就是高水位值
        val highWatermark = partitionState.fetchOffset
        // 告诉 Kafka 要把指定分区下副本截断到哪个位移值
        val truncationState = OffsetTruncationState(highWatermark, truncationCompleted = true)

        info(s"Truncating partition $tp to local high watermark $highWatermark")
        // 执行截断到高水位值
        if (doTruncate(tp, truncationState))
          fetchOffsets.put(tp, truncationState)
      }
    }

    // 更新这组分区的分区读取状态
    updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets)
  }

  // fetchedEpochs 根据获取的各分区的最新 leader epoch 的 offset 信息，对指定分区进行截断操作。
  //               获取的各分区的最新 leader epoch 的 offset 信息
  // latestEpochsForPartitions 指定分区的最新 leader epoch 信息
  // return 包含截断操作结果的 ResultWithPartitions 对象
  private def maybeTruncateToEpochEndOffsets(fetchedEpochs: Map[TopicPartition, EpochEndOffset],
                                             latestEpochsForPartitions: Map[TopicPartition, EpochData]): ResultWithPartitions[Map[TopicPartition, OffsetTruncationState]] = {
    // 保存要截断的分区的 offset 状态信息
    val fetchOffsets = mutable.HashMap.empty[TopicPartition, OffsetTruncationState]
    // 保存出现错误的分区
    val partitionsWithError = mutable.HashSet.empty[TopicPartition]

    fetchedEpochs.forKeyValue { (tp, leaderEpochOffset) =>
      // 遍历每个分区的最新 epoch offset 信息
      leaderEpochOffset.error match {
        case Errors.NONE =>
          // 获取截断的 offset 状态信息并加入到 fetchOffsets 中
          val offsetTruncationState = getOffsetTruncationState(tp, leaderEpochOffset)
          if(doTruncate(tp, offsetTruncationState))
            fetchOffsets.put(tp, offsetTruncationState)

        // 如果分区已被标记为 fenced，调用 onPartitionFenced 处理
        case Errors.FENCED_LEADER_EPOCH =>
          // 获取分区最新的 leader epoch 信息
          if (onPartitionFenced(tp, latestEpochsForPartitions.get(tp).flatMap {
            p =>
              if (p.currentLeaderEpoch.isPresent) Some(p.currentLeaderEpoch.get())
              else None
          })) partitionsWithError += tp

        // 如果出现其他错误，记录日志并加入到 partitionsWithError 中
        case error =>
          info(s"Retrying leaderEpoch request for partition $tp as the leader reported an error: $error")
          partitionsWithError += tp
      }
    }

    // 返回结果
    ResultWithPartitions(fetchOffsets, partitionsWithError)
  }

  /**
   * remove the partition if the partition state is NOT updated. Otherwise, keep the partition active.
   * @return true if the epoch in this thread is updated. otherwise, false
   */
  private def onPartitionFenced(tp: TopicPartition, requestEpoch: Option[Int]): Boolean = inLock(partitionMapLock) {
    Option(partitionStates.stateValue(tp)).exists { currentFetchState =>
      val currentLeaderEpoch = currentFetchState.currentLeaderEpoch
      if (requestEpoch.contains(currentLeaderEpoch)) {
        info(s"Partition $tp has an older epoch ($currentLeaderEpoch) than the current leader. Will await " +
          s"the new LeaderAndIsr state before resuming fetching.")
        markPartitionFailed(tp)
        false
      } else {
        info(s"Partition $tp has an new epoch ($currentLeaderEpoch) than the current leader. retry the partition later")
        true
      }
    }
  }

  // 发送 FETCH 请求给对应的 Leader 副本，并处理相应的 Response
  private def processFetchRequest(sessionPartitions: util.Map[TopicPartition, FetchRequest.PartitionData],
                                  fetchRequest: FetchRequest.Builder): Unit = {
    val partitionsWithError = mutable.Set[TopicPartition]()
    var responseData: Map[TopicPartition, FetchData] = Map.empty

    try {
      trace(s"Sending fetch request $fetchRequest")
      // 给 Leader 发送 FETCH 请求
      responseData = fetchFromLeader(fetchRequest)
    } catch {
      case t: Throwable =>
        if (isRunning) {
          warn(s"Error in response for fetch request $fetchRequest", t)
          inLock(partitionMapLock) {
            partitionsWithError ++= partitionStates.partitionSet.asScala
            // there is an error occurred while fetching partitions, sleep a while
            // note that `ReplicaFetcherThread.handlePartitionsWithError` will also introduce the same delay for every
            // partition with error effectively doubling the delay. It would be good to improve this.
            partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
          }
        }
    }
    // 更新请求发送速率指标
    fetcherStats.requestRate.mark()

    if (responseData.nonEmpty) {
      // process fetched data
      inLock(partitionMapLock) {
        // 遍历 Fetch 结果
        responseData.forKeyValue { (topicPartition, partitionData) =>
          Option(partitionStates.stateValue(topicPartition)).foreach { currentFetchState =>
            // It's possible that a partition is removed and re-added or truncated when there is a pending fetch request.
            // In this case, we only want to process the fetch response if the partition state is ready for fetch and
            // the current offset is the same as the offset requested.
            // 获取分区核心信息
            val fetchPartitionData = sessionPartitions.get(topicPartition)
            // 处理 Response 的条件：
            // 1. 要获取的位移值和之前已保存的下一条待获取位移值相等
            // 2. 当前分区处于可获取状态
            if (fetchPartitionData != null && fetchPartitionData.fetchOffset == currentFetchState.fetchOffset && currentFetchState.isReadyForFetch) {
              val requestEpoch = if (fetchPartitionData.currentLeaderEpoch.isPresent) Some(fetchPartitionData.currentLeaderEpoch.get().toInt) else None
              // 匹配分区数据的错误信息
              partitionData.error match {
                // 如果没有错误
                case Errors.NONE =>
                  try {
                    // Once we hand off the partition data to the subclass, we can't mess with it any more in this thread
                    // 交由子类完成 Response 的处理
                    val logAppendInfoOpt = processPartitionData(topicPartition, currentFetchState.fetchOffset,
                      partitionData)

                    logAppendInfoOpt.foreach { logAppendInfo =>
                      val validBytes = logAppendInfo.validBytes
                      val nextOffset = if (validBytes > 0) logAppendInfo.lastOffset + 1 else currentFetchState.fetchOffset
                      val lag = Math.max(0L, partitionData.highWatermark - nextOffset)
                      fetcherLagStats.getAndMaybePut(topicPartition).lag = lag

                      // ReplicaDirAlterThread may have removed topicPartition from the partitionStates after processing the partition data
                      if (validBytes > 0 && partitionStates.contains(topicPartition)) {
                        // Update partitionStates only if there is no exception during processPartitionData
                        val newFetchState = PartitionFetchState(nextOffset, Some(lag), currentFetchState.currentLeaderEpoch, state = Fetching)
                        partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
                        fetcherStats.byteRate.mark(validBytes)
                      }
                    }
                  } catch {
                    case ime@( _: CorruptRecordException | _: InvalidRecordException) =>
                      // we log the error and continue. This ensures two things
                      // 1. If there is a corrupt message in a topic partition, it does not bring the fetcher thread
                      //    down and cause other topic partition to also lag
                      // 2. If the message is corrupt due to a transient state in the log (truncation, partial writes
                      //    can cause this), we simply continue and should get fixed in the subsequent fetches
                      error(s"Found invalid messages during fetch for partition $topicPartition " +
                        s"offset ${currentFetchState.fetchOffset}", ime)
                      partitionsWithError += topicPartition
                    case e: KafkaStorageException =>
                      error(s"Error while processing data for partition $topicPartition " +
                        s"at offset ${currentFetchState.fetchOffset}", e)
                      markPartitionFailed(topicPartition)
                    case t: Throwable =>
                      // stop monitoring this partition and add it to the set of failed partitions
                      error(s"Unexpected error occurred while processing data for partition $topicPartition " +
                        s"at offset ${currentFetchState.fetchOffset}", t)
                      markPartitionFailed(topicPartition)
                  }
                // 如果读取位移值越界，通常是因为 Leader 发生变更
                case Errors.OFFSET_OUT_OF_RANGE =>
                  // 调整越界，主要办法是做截断
                  if (handleOutOfRangeError(topicPartition, currentFetchState, requestEpoch))
                  // 如果依然不能成功，加入到出错分区列表
                    partitionsWithError += topicPartition

                // 如果 Leader Epoch 值比 Leader 所在 Broker 上的 Epoch 值要新
                case Errors.UNKNOWN_LEADER_EPOCH =>
                  debug(s"Remote broker has a smaller leader epoch for partition $topicPartition than " +
                    s"this replica's current leader epoch of ${currentFetchState.currentLeaderEpoch}.")
                  partitionsWithError += topicPartition

                case Errors.FENCED_LEADER_EPOCH =>
                  if (onPartitionFenced(topicPartition, requestEpoch)) partitionsWithError += topicPartition

                // 如果 Leader 发生变更
                case Errors.NOT_LEADER_OR_FOLLOWER =>
                  debug(s"Remote broker is not the leader for partition $topicPartition, which could indicate " +
                    "that the partition is being moved")
                  // 加入到出错分区列表
                  partitionsWithError += topicPartition

                // 如果主题、分区不识别
                case Errors.UNKNOWN_TOPIC_OR_PARTITION =>
                  warn(s"Received ${Errors.UNKNOWN_TOPIC_OR_PARTITION} from the leader for partition $topicPartition. " +
                       "This error may be returned transiently when the partition is being created or deleted, but it is not " +
                       "expected to persist.")
                  // 加入到出错分区列表
                  partitionsWithError += topicPartition

                case _ =>
                  error(s"Error for partition $topicPartition at offset ${currentFetchState.fetchOffset}",
                    partitionData.error.exception)
                  partitionsWithError += topicPartition
              }
            }
          }
        }
      }
    }

    // 如果 divergingEndOffsets 集合中保存了支持截断操作的分区 Offset 信息，那么就调用 truncateOnFetchResponse 方法来执行相应的处理。
    if (partitionsWithError.nonEmpty) {
      // 处理出错分区列表
      handlePartitionsWithErrors(partitionsWithError, "processFetchRequest")
    }
  }

  def markPartitionsForTruncation(topicPartition: TopicPartition, truncationOffset: Long): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      Option(partitionStates.stateValue(topicPartition)).foreach { state =>
        val newState = PartitionFetchState(math.min(truncationOffset, state.fetchOffset),
          state.lag, state.currentLeaderEpoch, state.delay, state = Truncating)
        partitionStates.updateAndMoveToEnd(topicPartition, newState)
        partitionMapCond.signalAll()
      }
    } finally partitionMapLock.unlock()
  }

  private def markPartitionFailed(topicPartition: TopicPartition): Unit = {
    partitionMapLock.lock()
    try {
      failedPartitions.add(topicPartition)
      removePartitions(Set(topicPartition))
    } finally partitionMapLock.unlock()
    warn(s"Partition $topicPartition marked as failed")
  }

  def addPartitions(initialFetchStates: Map[TopicPartition, OffsetAndEpoch]): Set[TopicPartition] = {
    partitionMapLock.lockInterruptibly()
    try {
      failedPartitions.removeAll(initialFetchStates.keySet)

      initialFetchStates.forKeyValue { (tp, initialFetchState) =>
        // We can skip the truncation step iff the leader epoch matches the existing epoch
        val currentState = partitionStates.stateValue(tp)
        val updatedState = if (currentState != null && currentState.currentLeaderEpoch == initialFetchState.leaderEpoch) {
          currentState
        } else if (initialFetchState.offset < 0) {
          fetchOffsetAndTruncate(tp, initialFetchState.leaderEpoch)
        } else {
          PartitionFetchState(initialFetchState.offset, None, initialFetchState.leaderEpoch, state = Truncating)
        }
        partitionStates.updateAndMoveToEnd(tp, updatedState)
      }

      partitionMapCond.signalAll()
      initialFetchStates.keySet
    } finally partitionMapLock.unlock()
  }

  /**
    * Loop through all partitions, updating their fetch offset and maybe marking them as
    * truncation completed if their offsetTruncationState indicates truncation completed
    *
    * @param fetchOffsets the partitions to update fetch offset and maybe mark truncation complete
    */
  private def updateFetchOffsetAndMaybeMarkTruncationComplete(fetchOffsets: Map[TopicPartition, OffsetTruncationState]): Unit = {
    // 保存更新后的分区 fetch 状态信息
    val newStates: Map[TopicPartition, PartitionFetchState] = partitionStates.partitionStateMap.asScala
      .map { case (topicPartition, currentFetchState) =>
        // 匹配是否截断成功
        val maybeTruncationComplete = fetchOffsets.get(topicPartition) match {
          case Some(offsetTruncationState) =>
            val state = if (offsetTruncationState.truncationCompleted) Fetching else Truncating
            PartitionFetchState(offsetTruncationState.offset, currentFetchState.lag,
              currentFetchState.currentLeaderEpoch, currentFetchState.delay, state)
          case None => currentFetchState
        }
        (topicPartition, maybeTruncationComplete)
      }
    // 更新分区 fetch 状态信息
    partitionStates.set(newStates.asJava)
  }

  /**
   * Called from ReplicaFetcherThread and ReplicaAlterLogDirsThread maybeTruncate for each topic
   * partition. Returns truncation offset and whether this is the final offset to truncate to
   *
   * For each topic partition, the offset to truncate to is calculated based on leader's returned
   * epoch and offset:
   *  -- If the leader replied with undefined epoch offset, we must use the high watermark. This can
   *  happen if 1) the leader is still using message format older than KAFKA_0_11_0; 2) the follower
   *  requested leader epoch < the first leader epoch known to the leader.
   *  -- If the leader replied with the valid offset but undefined leader epoch, we truncate to
   *  leader's offset if it is lower than follower's Log End Offset. This may happen if the
   *  leader is on the inter-broker protocol version < KAFKA_2_0_IV0
   *  -- If the leader replied with leader epoch not known to the follower, we truncate to the
   *  end offset of the largest epoch that is smaller than the epoch the leader replied with, and
   *  send OffsetsForLeaderEpochRequest with that leader epoch. In a more rare case, where the
   *  follower was not tracking epochs smaller than the epoch the leader replied with, we
   *  truncate the leader's offset (and do not send any more leader epoch requests).
   *  -- Otherwise, truncate to min(leader's offset, end offset on the follower for epoch that
   *  leader replied with, follower's Log End Offset).
   *
   * @param tp                    Topic partition
   * @param leaderEpochOffset     Epoch end offset received from the leader for this topic partition
   */
  private def getOffsetTruncationState(tp: TopicPartition,
                                       leaderEpochOffset: EpochEndOffset): OffsetTruncationState = inLock(partitionMapLock) {
    if (leaderEpochOffset.endOffset == UNDEFINED_EPOCH_OFFSET) {
      // truncate to initial offset which is the high watermark for follower replica. For
      // future replica, it is either high watermark of the future replica or current
      // replica's truncation offset (when the current replica truncates, it forces future
      // replica's partition state to 'truncating' and sets initial offset to its truncation offset)
      warn(s"Based on replica's leader epoch, leader replied with an unknown offset in $tp. " +
           s"The initial fetch offset ${partitionStates.stateValue(tp).fetchOffset} will be used for truncation.")
      OffsetTruncationState(partitionStates.stateValue(tp).fetchOffset, truncationCompleted = true)
    } else if (leaderEpochOffset.leaderEpoch == UNDEFINED_EPOCH) {
      // either leader or follower or both use inter-broker protocol version < KAFKA_2_0_IV0
      // (version 0 of OffsetForLeaderEpoch request/response)
      warn(s"Leader or replica is on protocol version where leader epoch is not considered in the OffsetsForLeaderEpoch response. " +
           s"The leader's offset ${leaderEpochOffset.endOffset} will be used for truncation in $tp.")
      OffsetTruncationState(min(leaderEpochOffset.endOffset, logEndOffset(tp)), truncationCompleted = true)
    } else {
      // 获取 follower 副本的 logEndOffset，后面计算的截断位置必须小于或者等于该值
      val replicaEndOffset = logEndOffset(tp)

      // get (leader epoch, end offset) pair that corresponds to the largest leader epoch
      // less than or equal to the requested epoch.
      // 调用 endOffsetForEpoch 方法，在 follower 副本中找到小于或者等于 leader-lastEpoch 的最大 epoch 以及 epoch 结束偏移量，
      // 下面称为 follow-lastEpoch、follow-lastEpochEnd。
      endOffsetForEpoch(tp, leaderEpochOffset.leaderEpoch) match {
        case Some(OffsetAndEpoch(followerEndOffset, followerEpoch)) =>
          if (followerEpoch != leaderEpochOffset.leaderEpoch) {
            // the follower does not know about the epoch that leader replied with
            // we truncate to the end offset of the largest epoch that is smaller than the
            // epoch the leader replied with, and send another offset for leader epoch request
            // 如果 follow-lastEpoch 小于 leader-lastEpoch，则截断到 follow-lastEpochEnd 的位置
            val intermediateOffsetToTruncateTo = min(followerEndOffset, replicaEndOffset)
            info(s"Based on replica's leader epoch, leader replied with epoch ${leaderEpochOffset.leaderEpoch} " +
              s"unknown to the replica for $tp. " +
              s"Will truncate to $intermediateOffsetToTruncateTo and send another leader epoch request to the leader.")
            OffsetTruncationState(intermediateOffsetToTruncateTo, truncationCompleted = false)
          } else {
            // 如果 follow-lastEpoch 等于 leader-lastEpoch，则截断到 follow-lastEpochEnd、leader-lastEpochEnd 较小值对应的位置。
            val offsetToTruncateTo = min(followerEndOffset, leaderEpochOffset.endOffset)
            OffsetTruncationState(min(offsetToTruncateTo, replicaEndOffset), truncationCompleted = true)
          }
        case None =>
          // This can happen if the follower was not tracking leader epochs at that point (before the
          // upgrade, or if this broker is new). Since the leader replied with epoch <
          // requested epoch from follower, so should be safe to truncate to leader's
          // offset (this is the same behavior as post-KIP-101 and pre-KIP-279)
          warn(s"Based on replica's leader epoch, leader replied with epoch ${leaderEpochOffset.leaderEpoch} " +
            s"below any replica's tracked epochs for $tp. " +
            s"The leader's offset only ${leaderEpochOffset.endOffset} will be used for truncation.")
          // 如果找不到 follow-lastEpoch，则截断到 leader-lastEpochEnd
          OffsetTruncationState(min(leaderEpochOffset.endOffset, replicaEndOffset), truncationCompleted = true)
      }
    }
  }

  /**
   * Handle the out of range error. Return false if
   * 1) the request succeeded or
   * 2) was fenced and this thread haven't received new epoch,
   * which means we need not backoff and retry. True if there was a retriable error.
   */
  private def handleOutOfRangeError(topicPartition: TopicPartition,
                                    fetchState: PartitionFetchState,
                                    requestEpoch: Option[Int]): Boolean = {
    try {
      val newFetchState = fetchOffsetAndTruncate(topicPartition, fetchState.currentLeaderEpoch)
      partitionStates.updateAndMoveToEnd(topicPartition, newFetchState)
      info(s"Current offset ${fetchState.fetchOffset} for partition $topicPartition is " +
        s"out of range, which typically implies a leader change. Reset fetch offset to ${newFetchState.fetchOffset}")
      false
    } catch {
      case _: FencedLeaderEpochException =>
        onPartitionFenced(topicPartition, requestEpoch)

      case e @ (_ : UnknownTopicOrPartitionException |
                _ : UnknownLeaderEpochException |
                _ : NotLeaderOrFollowerException) =>
        info(s"Could not fetch offset for $topicPartition due to error: ${e.getMessage}")
        true

      case e: Throwable =>
        error(s"Error getting offset for partition $topicPartition", e)
        true
    }
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  protected def fetchOffsetAndTruncate(topicPartition: TopicPartition, currentLeaderEpoch: Int): PartitionFetchState = {
    val replicaEndOffset = logEndOffset(topicPartition)

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     */
    val leaderEndOffset = fetchLatestOffsetFromLeader(topicPartition, currentLeaderEpoch)
    if (leaderEndOffset < replicaEndOffset) {
      warn(s"Reset fetch offset for partition $topicPartition from $replicaEndOffset to current " +
        s"leader's latest offset $leaderEndOffset")
      truncate(topicPartition, OffsetTruncationState(leaderEndOffset, truncationCompleted = true))

      fetcherLagStats.getAndMaybePut(topicPartition).lag = 0
      PartitionFetchState(leaderEndOffset, Some(0), currentLeaderEpoch, state = Fetching)
    } else {
      /**
       * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
       * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
       * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
       * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
       * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
       * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
       *
       * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
       * start offset.
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
       * brokers and producers.
       *
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset.
       */
      val leaderStartOffset = fetchEarliestOffsetFromLeader(topicPartition, currentLeaderEpoch)
      warn(s"Reset fetch offset for partition $topicPartition from $replicaEndOffset to current " +
        s"leader's start offset $leaderStartOffset")
      val offsetToFetch = Math.max(leaderStartOffset, replicaEndOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset.
      if (leaderStartOffset > replicaEndOffset)
        truncateFullyAndStartAt(topicPartition, leaderStartOffset)

      val initialLag = leaderEndOffset - offsetToFetch
      fetcherLagStats.getAndMaybePut(topicPartition).lag = initialLag
      PartitionFetchState(offsetToFetch, Some(initialLag), currentLeaderEpoch, state = Fetching)
    }
  }

  def delayPartitions(partitions: Iterable[TopicPartition], delay: Long): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      for (partition <- partitions) {
        Option(partitionStates.stateValue(partition)).foreach { currentFetchState =>
          if (!currentFetchState.isDelayed) {
            partitionStates.updateAndMoveToEnd(partition, PartitionFetchState(currentFetchState.fetchOffset,
              currentFetchState.lag, currentFetchState.currentLeaderEpoch, Some(new DelayedItem(delay)), currentFetchState.state))
          }
        }
      }
      partitionMapCond.signalAll()
    } finally partitionMapLock.unlock()
  }

  def removePartitions(topicPartitions: Set[TopicPartition]): Unit = {
    partitionMapLock.lockInterruptibly()
    try {
      topicPartitions.foreach { topicPartition =>
        partitionStates.remove(topicPartition)
        fetcherLagStats.unregister(topicPartition)
      }
    } finally partitionMapLock.unlock()
  }

  def partitionCount: Int = {
    partitionMapLock.lockInterruptibly()
    try partitionStates.size
    finally partitionMapLock.unlock()
  }

  // Visible for testing
  private[server] def fetchState(topicPartition: TopicPartition): Option[PartitionFetchState] = inLock(partitionMapLock) {
    Option(partitionStates.stateValue(topicPartition))
  }

  private[server] def partitionsAndOffsets: Map[TopicPartition, InitialFetchState] = inLock(partitionMapLock) {
    partitionStates.partitionStateMap.asScala.map { case (topicPartition, currentFetchState) =>
      val initialFetchState = InitialFetchState(sourceBroker,
        currentLeaderEpoch = currentFetchState.currentLeaderEpoch,
        initOffset = currentFetchState.fetchOffset)
      topicPartition -> initialFetchState
    }
  }

  protected def toMemoryRecords(records: Records): MemoryRecords = {
    records match {
      case r: MemoryRecords => r
      case r: FileRecords =>
        val buffer = ByteBuffer.allocate(r.sizeInBytes)
        r.readInto(buffer, 0)
        MemoryRecords.readableRecords(buffer)
    }
  }

}

object AbstractFetcherThread {

  case class ReplicaFetch(partitionData: util.Map[TopicPartition, FetchRequest.PartitionData], fetchRequest: FetchRequest.Builder)
  case class ResultWithPartitions[R](result: R, partitionsWithError: Set[TopicPartition])

}

object FetcherMetrics {
  val ConsumerLag = "ConsumerLag"
  val RequestsPerSec = "RequestsPerSec"
  val BytesPerSec = "BytesPerSec"
}

class FetcherLagMetrics(metricId: ClientIdTopicPartition) extends KafkaMetricsGroup {

  private[this] val lagVal = new AtomicLong(-1L)
  private[this] val tags = Map(
    "clientId" -> metricId.clientId,
    "topic" -> metricId.topicPartition.topic,
    "partition" -> metricId.topicPartition.partition.toString)

  newGauge(FetcherMetrics.ConsumerLag, () => lagVal.get, tags)

  def lag_=(newLag: Long): Unit = {
    lagVal.set(newLag)
  }

  def lag = lagVal.get

  def unregister(): Unit = {
    removeMetric(FetcherMetrics.ConsumerLag, tags)
  }
}

class FetcherLagStats(metricId: ClientIdAndBroker) {
  private val valueFactory = (k: TopicPartition) => new FetcherLagMetrics(ClientIdTopicPartition(metricId.clientId, k))
  val stats = new Pool[TopicPartition, FetcherLagMetrics](Some(valueFactory))

  def getAndMaybePut(topicPartition: TopicPartition): FetcherLagMetrics = {
    stats.getAndMaybePut(topicPartition)
  }

  def unregister(topicPartition: TopicPartition): Unit = {
    val lagMetrics = stats.remove(topicPartition)
    if (lagMetrics != null) lagMetrics.unregister()
  }

  def unregister(): Unit = {
    stats.keys.toBuffer.foreach { key: TopicPartition =>
      unregister(key)
    }
  }
}

class FetcherStats(metricId: ClientIdAndBroker) extends KafkaMetricsGroup {
  val tags = Map("clientId" -> metricId.clientId,
    "brokerHost" -> metricId.brokerHost,
    "brokerPort" -> metricId.brokerPort.toString)

  val requestRate = newMeter(FetcherMetrics.RequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  val byteRate = newMeter(FetcherMetrics.BytesPerSec, "bytes", TimeUnit.SECONDS, tags)

  def unregister(): Unit = {
    removeMetric(FetcherMetrics.RequestsPerSec, tags)
    removeMetric(FetcherMetrics.BytesPerSec, tags)
  }

}

case class ClientIdTopicPartition(clientId: String, topicPartition: TopicPartition) {
  override def toString: String = s"$clientId-$topicPartition"
}

sealed trait ReplicaState
// 截断中
case object Truncating extends ReplicaState
// 获取中
case object Fetching extends ReplicaState

object PartitionFetchState {
  def apply(offset: Long, lag: Option[Long], currentLeaderEpoch: Int, state: ReplicaState): PartitionFetchState = {
    PartitionFetchState(offset, lag, currentLeaderEpoch, None, state)
  }
}


/**
 * case class to keep partition offset and its state(truncatingLog, delayed)
 * This represents a partition as being either:
 * (1) Truncating its log, for example having recently become a follower
 * (2) Delayed, for example due to an error, where we subsequently back off a bit
 * (3) ReadyForFetch, the is the active state where the thread is actively fetching data.
 */
case class PartitionFetchState(fetchOffset: Long,
                               lag: Option[Long],
                               currentLeaderEpoch: Int,
                               delay: Option[DelayedItem],
                               state: ReplicaState) {

  // 分区可获取的条件是副本处于 Fetching 且未被推迟执行
  def isReadyForFetch: Boolean = state == Fetching && !isDelayed

  // 副本处于 ISR 的条件：没有 lag
  def isReplicaInSync: Boolean = lag.isDefined && lag.get <= 0

  // 分区处于截断中状态的条件：副本处于 Truncating 状态且未被推迟执行
  def isTruncating: Boolean = state == Truncating && !isDelayed

  // 分区被推迟获取数据的条件：存在未过期的延迟任务
  def isDelayed: Boolean = delay.exists(_.getDelay(TimeUnit.MILLISECONDS) > 0)

  override def toString: String = {
    s"FetchState(fetchOffset=$fetchOffset" +
      s", currentLeaderEpoch=$currentLeaderEpoch" +
      s", state=$state" +
      s", lag=$lag" +
      s", delay=${delay.map(_.delayMs).getOrElse(0)}ms" +
      s")"
  }
}


// 位移值
// 截断是否完成状态值
case class OffsetTruncationState(offset: Long, truncationCompleted: Boolean) {

  def this(offset: Long) = this(offset, true)

  override def toString: String = "offset:%d-truncationCompleted:%b".format(offset, truncationCompleted)
}

case class OffsetAndEpoch(offset: Long, leaderEpoch: Int) {
  override def toString: String = {
    s"(offset=$offset, leaderEpoch=$leaderEpoch)"
  }
}
