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

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Implicits._
import kafka.utils.Pool
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = s"[acksPending: $acksPending, error: ${responseStatus.error.code}, " +
    s"startOffset: ${responseStatus.baseOffset}, requiredOffset: $requiredOffset]"
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = s"[requiredAcks: $produceRequiredAcks, partitionStatus: $produceStatus]"
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata,
                     replicaManager: ReplicaManager,
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit,
                     lockOpt: Option[Lock] = None)
  extends DelayedOperation(delayMs, lockOpt) {

  // first update the acks pending variable according to the error code
  // 遍历了 produceMetadata.produceStatus 对象中的每个键值对，并对每个状态进行处理。
  produceMetadata.produceStatus.forKeyValue { (topicPartition, status) =>
    // 对于每个状态，首先检查其 responseStatus.error 字段是否为 Errors.NONE。
    // 如果错误是 Errors.NONE，表示没有发生错误，则执行以下操作：
    if (status.responseStatus.error == Errors.NONE) {
      // Timeout error state will be cleared when required acks are received
      // 将 status.acksPending 设置为 true，表示等待确认。
      status.acksPending = true
      // 将 status.responseStatus.error 设置为 Errors.REQUEST_TIMED_OUT，表示请求超时。
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      // 如果错误不是 Errors.NONE，表示发生了错误，则执行以下操作：
      // 将 status.acksPending 设置为 false，表示不需要等待确认。
      status.acksPending = false
    }

    trace(s"Initial partition status for $topicPartition is $status")
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: This broker is no longer the leader: set an error in response
   * Case B: This broker is the leader:
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   B.2 - Otherwise, set the response with no error.
   */
  // 尝试完成任务
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    // 为每个分区检查是否仍有待确认的消息
    produceMetadata.produceStatus.forKeyValue { (topicPartition, status) =>
      trace(s"Checking produce satisfaction for $topicPartition, current status $status")
      // skip those partitions that have already been satisfied
      // 跳过已满足的分区
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartitionOrError(topicPartition) match {
          case Left(err) =>
            // Case A
            // Case A : 对于无可用分区的情况，将 hasEnough 设置为 false，错误设置为对应错误
            (false, err)

          // Case B : 验证分区是否满足 ACK，如果满足，则将 hasEnough 设置为 true，否则为 false
          case Right(partition) =>
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
        }

        // Case B.1 || B.2
        // 处理 Case B.1 和 B.2 的情况，并标记为不再需要确认
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        }
      }
    }

    // check if every partition has satisfied at least one of case A or B
    // 检查每个分区是否已满足 Case A 和 Case B 中的至少一种情况
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  // 在任务超时时执行的操作
  override def onExpiration(): Unit = {
    // 遍历 produceMetadata.produceStatus，查找仍有待确认消息的分区
    produceMetadata.produceStatus.forKeyValue { (topicPartition, status) =>
      if (status.acksPending) {
        // 输出调试日志，显示哪些分区的请求已过期
        debug(s"Expiring produce request for partition $topicPartition with status $status")
        // 记录延迟产生指标，这些指标用于度量消息发送的延迟。
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  // 任务完成时执行的操作
  override def onComplete(): Unit = {
    val responseStatus = produceMetadata.produceStatus.map { case (k, status) => k -> status.responseStatus }
    // 调用 responseCallback 方法，将 responseStatus 作为参数传递给回调函数
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition): Unit = {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

