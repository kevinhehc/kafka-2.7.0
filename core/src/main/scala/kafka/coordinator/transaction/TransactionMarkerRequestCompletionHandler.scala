/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator.transaction

import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, RequestCompletionHandler}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.WriteTxnMarkersResponse

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class TransactionMarkerRequestCompletionHandler(brokerId: Int,
                                                txnStateManager: TransactionStateManager,
                                                txnMarkerChannelManager: TransactionMarkerChannelManager,
                                                txnIdAndMarkerEntries: java.util.List[TxnIdAndMarkerEntry]) extends RequestCompletionHandler with Logging {

  // 设置日志标识
  this.logIdent = "[Transaction Marker Request Completion Handler " + brokerId + "]: "

  override def onComplete(response: ClientResponse): Unit = {
    val requestHeader = response.requestHeader
    val correlationId = requestHeader.correlationId
    // 如果响应中断开了连接
    if (response.wasDisconnected) {
      // 取消请求，并处理其关联的事务标记
      trace(s"Cancelled request with header $requestHeader due to node ${response.destination} being disconnected")

      for (txnIdAndMarker <- txnIdAndMarkerEntries.asScala) {
        val transactionalId = txnIdAndMarker.txnId
        val txnMarker = txnIdAndMarker.txnMarkerEntry

        // 获取当前事务的状态
        txnStateManager.getTransactionState(transactionalId) match {

          case Left(Errors.NOT_COORDINATOR) =>
            // 当前节点不再是该事务的协调器，取消发送事务标记
            info(s"I am no longer the coordinator for $transactionalId; cancel sending transaction markers $txnMarker to the brokers")

            txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)

          case Left(Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
            // 加载包含该事务的分区，取消发送事务标记
            info(s"I am loading the transaction partition that contains $transactionalId which means the current markers have to be obsoleted; " +
              s"cancel sending transaction markers $txnMarker to the brokers")

            txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)

          case Left(unexpectedError) =>
            // 处理其他未处理的错误
            throw new IllegalStateException(s"Unhandled error $unexpectedError when fetching current transaction state")

          case Right(None) =>
            // 协调器仍然拥有该事务分区，但缓存中没有元数据，这是不应该发生的
            throw new IllegalStateException(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
              s"no metadata in the cache; this is not expected")

          case Right(Some(epochAndMetadata)) =>
            if (epochAndMetadata.coordinatorEpoch != txnMarker.coordinatorEpoch) {
              // coordinator epoch has changed, just cancel it from the purgatory
              // 协调器的epoch已经改变，取消发送事务标记
              info(s"Transaction coordinator epoch for $transactionalId has changed from ${txnMarker.coordinatorEpoch} to " +
                s"${epochAndMetadata.coordinatorEpoch}; cancel sending transaction markers $txnMarker to the brokers")

              txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)
            } else {
              // re-enqueue the markers with possibly new destination brokers
              // 使用可能有新目标代理的方式重新排队标记
              trace(s"Re-enqueuing ${txnMarker.transactionResult} transaction markers for transactional id $transactionalId " +
                s"under coordinator epoch ${txnMarker.coordinatorEpoch}")

              txnMarkerChannelManager.addTxnMarkersToBrokerQueue(transactionalId,
                txnMarker.producerId,
                txnMarker.producerEpoch,
                txnMarker.transactionResult,
                txnMarker.coordinatorEpoch,
                txnMarker.partitions.asScala.toSet)
            }
        }
      }
    } else {
      // 接收到写入事务标记的响应
      debug(s"Received WriteTxnMarker response $response from node ${response.destination} with correlation id $correlationId")

      val writeTxnMarkerResponse = response.responseBody.asInstanceOf[WriteTxnMarkersResponse]

      for (txnIdAndMarker <- txnIdAndMarkerEntries.asScala) {
        val transactionalId = txnIdAndMarker.txnId
        val txnMarker = txnIdAndMarker.txnMarkerEntry
        val errors = writeTxnMarkerResponse.errors(txnMarker.producerId)

        if (errors == null)
          throw new IllegalStateException(s"WriteTxnMarkerResponse does not contain expected error map for producer id ${txnMarker.producerId}")

        txnStateManager.getTransactionState(transactionalId) match {
          case Left(Errors.NOT_COORDINATOR) =>
            // 当前节点不再是该事务的协调器，取消发送事务标记
            info(s"I am no longer the coordinator for $transactionalId; cancel sending transaction markers $txnMarker to the brokers")

            txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)

          case Left(Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
            // 加载包含该事务的分区，取消发送事务标记
            info(s"I am loading the transaction partition that contains $transactionalId which means the current markers have to be obsoleted; " +
              s"cancel sending transaction markers $txnMarker to the brokers")

            txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)

          case Left(unexpectedError) =>
            // 处理其他未处理的错误
            throw new IllegalStateException(s"Unhandled error $unexpectedError when fetching current transaction state")

          case Right(None) =>
            // 协调器仍然拥有该事务分区，但缓存中没有元数据，这是不应该发生的
            throw new IllegalStateException(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
              s"no metadata in the cache; this is not expected")

          case Right(Some(epochAndMetadata)) =>
            val txnMetadata = epochAndMetadata.transactionMetadata
            val retryPartitions: mutable.Set[TopicPartition] = mutable.Set.empty[TopicPartition]
            var abortSending: Boolean = false

            if (epochAndMetadata.coordinatorEpoch != txnMarker.coordinatorEpoch) {
              // coordinator epoch has changed, just cancel it from the purgatory
              // 协调器的epoch已经改变，取消发送事务标记
              info(s"Transaction coordinator epoch for $transactionalId has changed from ${txnMarker.coordinatorEpoch} to " +
                s"${epochAndMetadata.coordinatorEpoch}; cancel sending transaction markers $txnMarker to the brokers")

              txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)
              abortSending = true
            } else {
              txnMetadata.inLock {
                for ((topicPartition, error) <- errors.asScala) {
                  error match {
                    case Errors.NONE =>
                      // 从事务元数据中移除该分区
                      txnMetadata.removePartition(topicPartition)

                    // 这些都是意外的且致命的错误
                    case Errors.CORRUPT_MESSAGE |
                         Errors.MESSAGE_TOO_LARGE |
                         Errors.RECORD_LIST_TOO_LARGE |
                         Errors.INVALID_REQUIRED_ACKS => // these are all unexpected and fatal errors

                      throw new IllegalStateException(s"Received fatal error ${error.exceptionName} while sending txn marker for $transactionalId")

                    // 这些是可重试的错误
                    case Errors.UNKNOWN_TOPIC_OR_PARTITION |
                         Errors.NOT_LEADER_OR_FOLLOWER |
                         Errors.NOT_ENOUGH_REPLICAS |
                         Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND |
                         Errors.REQUEST_TIMED_OUT |
                         Errors.KAFKA_STORAGE_ERROR => // these are retriable errors

                      info(s"Sending $transactionalId's transaction marker for partition $topicPartition has failed with error ${error.exceptionName}, retrying " +
                        s"with current coordinator epoch ${epochAndMetadata.coordinatorEpoch}")

                      retryPartitions += topicPartition

                    // producer或coordinator的epoch已经改变，可以忽略该事务
                    case Errors.INVALID_PRODUCER_EPOCH |
                         Errors.TRANSACTION_COORDINATOR_FENCED => // producer or coordinator epoch has changed, this txn can now be ignored

                      info(s"Sending $transactionalId's transaction marker for partition $topicPartition has permanently failed with error ${error.exceptionName} " +
                        s"with the current coordinator epoch ${epochAndMetadata.coordinatorEpoch}; cancel sending any more transaction markers $txnMarker to the brokers")

                      txnMarkerChannelManager.removeMarkersForTxnId(transactionalId)
                      abortSending = true

                    case Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT |
                         Errors.UNSUPPORTED_VERSION =>
                      // The producer would have failed to send data to the failed topic so we can safely remove the partition
                      // from the set waiting for markers
                      info(s"Sending $transactionalId's transaction marker from partition $topicPartition has failed with " +
                        s" ${error.name}. This partition will be removed from the set of partitions" +
                        s" waiting for completion")
                      txnMetadata.removePartition(topicPartition)

                    case other =>
                      throw new IllegalStateException(s"Unexpected error ${other.exceptionName} while sending txn marker for $transactionalId")
                  }
                }
              }
            }

            if (!abortSending) {
              if (retryPartitions.nonEmpty) {
                debug(s"Re-enqueuing ${txnMarker.transactionResult} transaction markers for transactional id $transactionalId " +
                  s"under coordinator epoch ${txnMarker.coordinatorEpoch}")

                // re-enqueue with possible new leaders of the partitions
                txnMarkerChannelManager.addTxnMarkersToBrokerQueue(
                  transactionalId,
                  txnMarker.producerId,
                  txnMarker.producerEpoch,
                  txnMarker.transactionResult,
                  txnMarker.coordinatorEpoch,
                  retryPartitions.toSet)
              } else {
                txnMarkerChannelManager.maybeWriteTxnCompletion(transactionalId)
              }
            }
        }
      }
    }
  }
}
