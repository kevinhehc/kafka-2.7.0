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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.{KafkaConfig, MetadataCache, ReplicaManager}
import kafka.utils.{Logging, Scheduler}
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.utils.{LogContext, ProducerIdAndEpoch, Time}

object TransactionCoordinator {

  def apply(config: KafkaConfig,
            replicaManager: ReplicaManager,
            scheduler: Scheduler,
            zkClient: KafkaZkClient,
            metrics: Metrics,
            metadataCache: MetadataCache,
            time: Time): TransactionCoordinator = {

    val txnConfig = TransactionConfig(config.transactionalIdExpirationMs,
      config.transactionMaxTimeoutMs,
      config.transactionTopicPartitions,
      config.transactionTopicReplicationFactor,
      config.transactionTopicSegmentBytes,
      config.transactionsLoadBufferSize,
      config.transactionTopicMinISR,
      config.transactionAbortTimedOutTransactionCleanupIntervalMs,
      config.transactionRemoveExpiredTransactionalIdCleanupIntervalMs,
      config.requestTimeoutMs)

    val producerIdManager = new ProducerIdManager(config.brokerId, zkClient)
    val txnStateManager = new TransactionStateManager(config.brokerId, zkClient, scheduler, replicaManager, txnConfig,
      time, metrics)

    val logContext = new LogContext(s"[TransactionCoordinator id=${config.brokerId}] ")
    val txnMarkerChannelManager = TransactionMarkerChannelManager(config, metrics, metadataCache, txnStateManager,
      time, logContext)

    new TransactionCoordinator(config.brokerId, txnConfig, scheduler, producerIdManager, txnStateManager, txnMarkerChannelManager,
      time, logContext)
  }

  private def initTransactionError(error: Errors): InitProducerIdResult = {
    InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error)
  }

  private def initTransactionMetadata(txnMetadata: TxnTransitMetadata): InitProducerIdResult = {
    InitProducerIdResult(txnMetadata.producerId, txnMetadata.producerEpoch, Errors.NONE)
  }
}

/**
 * Transaction coordinator handles message transactions sent by producers and communicate with brokers
 * to update ongoing transaction's status.
 *
 * Each Kafka server instantiates a transaction coordinator which is responsible for a set of
 * producers. Producers with specific transactional ids are assigned to their corresponding coordinators;
 * Producers with no specific transactional id may talk to a random broker as their coordinators.
 */
class TransactionCoordinator(brokerId: Int,
                             txnConfig: TransactionConfig,
                             scheduler: Scheduler,
                             producerIdManager: ProducerIdManager,
                             txnManager: TransactionStateManager,
                             txnMarkerChannelManager: TransactionMarkerChannelManager,
                             time: Time,
                             logContext: LogContext) extends Logging {
  this.logIdent = logContext.logPrefix

  import TransactionCoordinator._

  type InitProducerIdCallback = InitProducerIdResult => Unit
  type AddPartitionsCallback = Errors => Unit
  type EndTxnCallback = Errors => Unit
  type ApiResult[T] = Either[Errors, T]

  /* Active flag of the coordinator */
  private val isActive = new AtomicBoolean(false)

  def handleInitProducerId(transactionalId: String,
                           transactionTimeoutMs: Int,
                           expectedProducerIdAndEpoch: Option[ProducerIdAndEpoch],
                           responseCallback: InitProducerIdCallback): Unit = {

    // 如果 transactionalId 为 null，生成一个新的 producerId，并返回结果给客户端
    if (transactionalId == null) {
      // if the transactional id is null, then always blindly accept the request
      // and return a new producerId from the producerId manager
      val producerId = producerIdManager.generateProducerId()
      responseCallback(InitProducerIdResult(producerId, producerEpoch = 0, Errors.NONE))
    } else if (transactionalId.isEmpty) {
      // 如果 transactionalId 为空字符串，返回 INVALID_REQUEST 错误给客户端
      // if transactional id is empty then return error as invalid request. This is
      // to make TransactionCoordinator's behavior consistent with producer client
      responseCallback(initTransactionError(Errors.INVALID_REQUEST))
    } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)) {
      // 检查 transactionTimeoutMs 是否有效
      // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
      responseCallback(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT))
    } else {
      // 处理正常情况

      // 获取 transactionalId 对应的协调者状态和元数据
      val coordinatorEpochAndMetadata = txnManager.getTransactionState(transactionalId).flatMap {
        case None =>
          // 如果不存在该 transactionalId 对应的状态和元数据，则生成一个新的 producerId
          val producerId = producerIdManager.generateProducerId()
          // 构建事务元数据
          val createdMetadata = new TransactionMetadata(transactionalId = transactionalId,
            producerId = producerId,
            lastProducerId = RecordBatch.NO_PRODUCER_ID,
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
            txnTimeoutMs = transactionTimeoutMs,
            state = Empty,
            topicPartitions = collection.mutable.Set.empty[TopicPartition],
            txnLastUpdateTimestamp = time.milliseconds())
          // 添加新的状态和元数据到事务管理器
          txnManager.putTransactionStateIfNotExists(createdMetadata)

        case Some(epochAndTxnMetadata) => Right(epochAndTxnMetadata)
      }

      // 准备 InitProducerId 操作，并返回结果
      val result: ApiResult[(Int, TxnTransitMetadata)] = coordinatorEpochAndMetadata.flatMap {
        existingEpochAndMetadata =>
          val coordinatorEpoch = existingEpochAndMetadata.coordinatorEpoch
          val txnMetadata = existingEpochAndMetadata.transactionMetadata

          txnMetadata.inLock {
            prepareInitProducerIdTransit(transactionalId, transactionTimeoutMs, coordinatorEpoch, txnMetadata,
              expectedProducerIdAndEpoch)
          }
      }

      // 处理 InitProducerId 的结果
      result match {
        case Left(error) =>
          responseCallback(initTransactionError(error))

        case Right((coordinatorEpoch, newMetadata)) =>
          // 如果新的 metadata 的状态是 PrepareEpochFence，终止正在进行中的 transaction
          if (newMetadata.txnState == PrepareEpochFence) {
            // abort the ongoing transaction and then return CONCURRENT_TRANSACTIONS to let client wait and retry
            // 终止 transaction，并返回 CONCURRENT_TRANSACTIONS 给客户端
            def sendRetriableErrorCallback(error: Errors): Unit = {
              if (error != Errors.NONE) {
                responseCallback(initTransactionError(error))
              } else {
                responseCallback(initTransactionError(Errors.CONCURRENT_TRANSACTIONS))
              }
            }

            endTransaction(transactionalId,
              newMetadata.producerId,
              newMetadata.producerEpoch,
              TransactionResult.ABORT,
              isFromClient = false,
              sendRetriableErrorCallback)
          } else {
            // 如果新的 metadata 的状态不是 PrepareEpochFence，将 metadata 添加到事务日志中，并返回结果给客户端
            def sendPidResponseCallback(error: Errors): Unit = {
              if (error == Errors.NONE) {
                info(s"Initialized transactionalId $transactionalId with producerId ${newMetadata.producerId} and producer " +
                  s"epoch ${newMetadata.producerEpoch} on partition " +
                  s"${Topic.TRANSACTION_STATE_TOPIC_NAME}-${txnManager.partitionFor(transactionalId)}")
                responseCallback(initTransactionMetadata(newMetadata))
              } else {
                info(s"Returning $error error code to client for $transactionalId's InitProducerId request")
                responseCallback(initTransactionError(error))
              }
            }

            // 将 metadata 添加到事务日志中
            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendPidResponseCallback)
          }
      }
    }
  }

  private def prepareInitProducerIdTransit(transactionalId: String,
                                          transactionTimeoutMs: Int,
                                          coordinatorEpoch: Int,
                                          txnMetadata: TransactionMetadata,
                                          expectedProducerIdAndEpoch: Option[ProducerIdAndEpoch]): ApiResult[(Int, TxnTransitMetadata)] = {

    def isValidProducerId(producerIdAndEpoch: ProducerIdAndEpoch): Boolean = {
      // If a producer ID and epoch are provided by the request, fence the producer unless one of the following is true:
      //   1. The producer epoch is equal to -1, which implies that the metadata was just created. This is the case of a
      //      producer recovering from an UNKNOWN_PRODUCER_ID error, and it is safe to return the newly-generated
      //      producer ID.
      //   2. The expected producer ID matches the ID in current metadata (the epoch will be checked when we try to
      //      increment it)
      //   3. The expected producer ID matches the previous one and the expected epoch is exhausted, in which case this
      //      could be a retry after a valid epoch bump that the producer never received the response for
      txnMetadata.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH ||
        producerIdAndEpoch.producerId == txnMetadata.producerId ||
        (producerIdAndEpoch.producerId == txnMetadata.lastProducerId && TransactionMetadata.isEpochExhausted(producerIdAndEpoch.epoch))
    }

    if (txnMetadata.pendingTransitionInProgress) {
      // return a retriable exception to let the client backoff and retry
      Left(Errors.CONCURRENT_TRANSACTIONS)
    }
    else if (!expectedProducerIdAndEpoch.forall(isValidProducerId)) {
      Left(Errors.PRODUCER_FENCED)
    } else {
      // caller should have synchronized on txnMetadata already
      txnMetadata.state match {
        case PrepareAbort | PrepareCommit =>
          // reply to client and let it backoff and retry
          Left(Errors.CONCURRENT_TRANSACTIONS)

        case CompleteAbort | CompleteCommit | Empty =>
          val transitMetadataResult =
            // If the epoch is exhausted and the expected epoch (if provided) matches it, generate a new producer ID
            if (txnMetadata.isProducerEpochExhausted &&
                expectedProducerIdAndEpoch.forall(_.epoch == txnMetadata.producerEpoch)) {
              val newProducerId = producerIdManager.generateProducerId()
              Right(txnMetadata.prepareProducerIdRotation(newProducerId, transactionTimeoutMs, time.milliseconds(),
                expectedProducerIdAndEpoch.isDefined))
            } else {
              txnMetadata.prepareIncrementProducerEpoch(transactionTimeoutMs, expectedProducerIdAndEpoch.map(_.epoch),
                time.milliseconds())
            }

          transitMetadataResult match {
            case Right(transitMetadata) => Right((coordinatorEpoch, transitMetadata))
            case Left(err) => Left(err)
          }

        case Ongoing =>
          // indicate to abort the current ongoing txn first. Note that this epoch is never returned to the
          // user. We will abort the ongoing transaction and return CONCURRENT_TRANSACTIONS to the client.
          // This forces the client to retry, which will ensure that the epoch is bumped a second time. In
          // particular, if fencing the current producer exhausts the available epochs for the current producerId,
          // then when the client retries, we will generate a new producerId.
          Right(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())

        case Dead | PrepareEpochFence =>
          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
            s"This is illegal as we should never have transitioned to this state."
          fatal(errorMsg)
          throw new IllegalStateException(errorMsg)

      }
    }
  }

  // 处理向事务添加分区的请求
  def handleAddPartitionsToTransaction(transactionalId: String, // 事务ID
                                       producerId: Long,// 生产者ID
                                       producerEpoch: Short, // 生产者Epoch
                                       partitions: collection.Set[TopicPartition], // 要添加的分区集合
                                       responseCallback: AddPartitionsCallback): Unit = { // 添加分区回调函数

    // 如果事务ID为空，则返回INVALID_REQUEST错误码
    if (transactionalId == null || transactionalId.isEmpty) {
      debug(s"Returning ${Errors.INVALID_REQUEST} error code to client for $transactionalId's AddPartitions request")
      responseCallback(Errors.INVALID_REQUEST)
    } else {
      // try to update the transaction metadata and append the updated metadata to txn log;
      // if there is no such metadata treat it as invalid producerId mapping error.
      // 尝试更新事务元数据并将更新后的元数据附加到事务日志上；
      // 如果不存在此类元数据，则将其视为无效的生产者ID映射错误。
      val result: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
        case None => Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndMetadata) =>
          val coordinatorEpoch = epochAndMetadata.coordinatorEpoch
          val txnMetadata = epochAndMetadata.transactionMetadata

          // generate the new transaction metadata with added partitions
          // 生成新的添加分区元数据
          txnMetadata.inLock {
            if (txnMetadata.producerId != producerId) {
              // 如果生产者ID不匹配，则返回INVALID_PRODUCER_ID_MAPPING错误
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            } else if (txnMetadata.producerEpoch != producerEpoch) {
              // 如果生产者周期不匹配，则返回PRODUCER_FENCED错误码
              Left(Errors.PRODUCER_FENCED)
            } else if (txnMetadata.pendingTransitionInProgress) {
              // return a retriable exception to let the client backoff and retry
              // 如果有挂起的事务，则返回CONCURRENT_TRANSACTIONS错误码
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == PrepareCommit || txnMetadata.state == PrepareAbort) {
              // 如果当前正在处理提交或中止操作，则返回CONCURRENT_TRANSACTIONS错误码
              Left(Errors.CONCURRENT_TRANSACTIONS)
            } else if (txnMetadata.state == Ongoing && partitions.subsetOf(txnMetadata.topicPartitions)) {
              // this is an optimization: if the partitions are already in the metadata reply OK immediately
              // 如果分区已经在元数据中，则返回NONE错误码（优化）
              Left(Errors.NONE)
            } else {
              // 正常情况下，生成新的（添加分区）元数据
              Right(coordinatorEpoch, txnMetadata.prepareAddPartitions(partitions.toSet, time.milliseconds()))
            }
          }
      }

      result match {
        // 如果处理出错，则返回相应的错误码回调客户端
        case Left(err) =>
          debug(s"Returning $err error code to client for $transactionalId's AddPartitions request")
          responseCallback(err)

        // 如果处理成功，则将新的元数据附加到事务日志中，并回调客户端
        case Right((coordinatorEpoch, newMetadata)) =>
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, responseCallback)
      }
    }
  }

  /**
   * Load state from the given partition and begin handling requests for groups which map to this partition.
   *
   * @param txnTopicPartitionId The partition that we are now leading
   * @param coordinatorEpoch The partition coordinator (or leader) epoch from the received LeaderAndIsr request
   */
  def onElection(txnTopicPartitionId: Int, coordinatorEpoch: Int): Unit = {
    // The operations performed during immigration must be resilient to any previous errors we saw or partial state we
    // left off during the unloading phase. Ensure we remove all associated state for this partition before we continue
    // loading it.
    txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)

    // Now load the partition.
    txnManager.loadTransactionsForTxnTopicPartition(txnTopicPartitionId, coordinatorEpoch,
      txnMarkerChannelManager.addTxnMarkersToSend)
  }

  /**
   * Clear coordinator caches for the given partition after giving up leadership.
   *
   * @param txnTopicPartitionId The partition that we are no longer leading
   * @param coordinatorEpoch The partition coordinator (or leader) epoch, which may be absent if we
   *                         are resigning after receiving a StopReplica request from the controller
   */
  def onResignation(txnTopicPartitionId: Int, coordinatorEpoch: Option[Int]): Unit = {
    coordinatorEpoch match {
      case Some(epoch) =>
        txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId, epoch)
      case None =>
        txnManager.removeTransactionsForTxnTopicPartition(txnTopicPartitionId)
    }
    txnMarkerChannelManager.removeMarkersForTxnTopicPartition(txnTopicPartitionId)
  }

  private def logInvalidStateTransitionAndReturnError(transactionalId: String,
                                                      transactionState: TransactionState,
                                                      transactionResult: TransactionResult) = {
    debug(s"TransactionalId: $transactionalId's state is $transactionState, but received transaction " +
      s"marker result to send: $transactionResult")
    Left(Errors.INVALID_TXN_STATE)
  }

  def handleEndTransaction(transactionalId: String,
                           producerId: Long,
                           producerEpoch: Short,
                           txnMarkerResult: TransactionResult,
                           responseCallback: EndTxnCallback): Unit = {
    // 可以看到最终会调用 endTransaction 来完成事务
    endTransaction(transactionalId,
      producerId,
      producerEpoch,
      txnMarkerResult,
      isFromClient = true,
      responseCallback)
  }

  private def endTransaction(transactionalId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             txnMarkerResult: TransactionResult,
                             isFromClient: Boolean,
                             responseCallback: EndTxnCallback): Unit = {
    var isEpochFence = false
    if (transactionalId == null || transactionalId.isEmpty)
      responseCallback(Errors.INVALID_REQUEST)
    else {
      // 处理正常情况下的逻辑，检查协调者事务状态当前是否可以切换到 PrepareCommit 状态。
      val preAppendResult: ApiResult[(Int, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
        case None =>
          Left(Errors.INVALID_PRODUCER_ID_MAPPING)

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val coordinatorEpoch = epochAndTxnMetadata.coordinatorEpoch

          txnMetadata.inLock {
            if (txnMetadata.producerId != producerId)
              Left(Errors.INVALID_PRODUCER_ID_MAPPING)
            // Strict equality is enforced on the client side requests, as they shouldn't bump the producer epoch.
            else if ((isFromClient && producerEpoch != txnMetadata.producerEpoch) || producerEpoch < txnMetadata.producerEpoch)
              Left(Errors.PRODUCER_FENCED)
            else if (txnMetadata.pendingTransitionInProgress && txnMetadata.pendingState.get != PrepareEpochFence)
              Left(Errors.CONCURRENT_TRANSACTIONS)
            else txnMetadata.state match {
              // 如果事务正在进行中
              case Ongoing =>
                // 判断结果是准备提交还是准备回滚
                val nextState = if (txnMarkerResult == TransactionResult.COMMIT)
                  PrepareCommit
                else
                  PrepareAbort

                if (nextState == PrepareAbort && txnMetadata.pendingState.contains(PrepareEpochFence)) {
                  // We should clear the pending state to make way for the transition to PrepareAbort and also bump
                  // the epoch in the transaction metadata we are about to append.
                  isEpochFence = true
                  txnMetadata.pendingState = None
                  txnMetadata.producerEpoch = producerEpoch
                  txnMetadata.lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH
                }

                Right(coordinatorEpoch, txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds()))
              case CompleteCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case CompleteAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.NONE)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareCommit =>
                if (txnMarkerResult == TransactionResult.COMMIT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case PrepareAbort =>
                if (txnMarkerResult == TransactionResult.ABORT)
                  Left(Errors.CONCURRENT_TRANSACTIONS)
                else
                  logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Empty =>
                logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
              case Dead | PrepareEpochFence =>
                val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                  s"This is illegal as we should never have transitioned to this state."
                fatal(errorMsg)
                throw new IllegalStateException(errorMsg)

            }
          }
      }

      // 匹配状态结果
      preAppendResult match {
        case Left(err) =>
          debug(s"Aborting append of $txnMarkerResult to transaction log with coordinator and returning $err error to client for $transactionalId's EndTransaction request")
          responseCallback(err)

        case Right((coordinatorEpoch, newMetadata)) =>
          // 如果是执行到这里了，表示协调者事务状态可以切换到 PrepareCommit 状态，先定义sendTxnMarkersCallback 回调函数
          def sendTxnMarkersCallback(error: Errors): Unit = {
            if (error == Errors.NONE) {
              // 检查协调者事务状态当前是否可以切换到 CompleteCommit 状态
              val preSendResult: ApiResult[(TransactionMetadata, TxnTransitMetadata)] = txnManager.getTransactionState(transactionalId).flatMap {
                case None =>
                  val errorMsg = s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                    s"no metadata in the cache; this is not expected"
                  fatal(errorMsg)
                  throw new IllegalStateException(errorMsg)

                case Some(epochAndMetadata) =>
                  if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                    val txnMetadata = epochAndMetadata.transactionMetadata
                    txnMetadata.inLock {
                      if (txnMetadata.producerId != producerId)
                        Left(Errors.INVALID_PRODUCER_ID_MAPPING)
                      else if (txnMetadata.producerEpoch != producerEpoch)
                        Left(Errors.PRODUCER_FENCED)
                      else if (txnMetadata.pendingTransitionInProgress)
                        Left(Errors.CONCURRENT_TRANSACTIONS)
                      else txnMetadata.state match {
                        case Empty| Ongoing | CompleteCommit | CompleteAbort =>
                          logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                        case PrepareCommit =>
                          if (txnMarkerResult != TransactionResult.COMMIT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case PrepareAbort =>
                          if (txnMarkerResult != TransactionResult.ABORT)
                            logInvalidStateTransitionAndReturnError(transactionalId, txnMetadata.state, txnMarkerResult)
                          else
                            Right(txnMetadata, txnMetadata.prepareComplete(time.milliseconds()))
                        case Dead | PrepareEpochFence =>
                          val errorMsg = s"Found transactionalId $transactionalId with state ${txnMetadata.state}. " +
                            s"This is illegal as we should never have transitioned to this state."
                          fatal(errorMsg)
                          throw new IllegalStateException(errorMsg)

                      }
                    }
                  } else {
                    debug(s"The transaction coordinator epoch has changed to ${epochAndMetadata.coordinatorEpoch} after $txnMarkerResult was " +
                      s"successfully appended to the log for $transactionalId with old epoch $coordinatorEpoch")
                    Left(Errors.NOT_COORDINATOR)
                  }
              }

              preSendResult match {
                case Left(err) =>
                  info(s"Aborting sending of transaction markers after appended $txnMarkerResult to transaction log and returning $err error to client for $transactionalId's EndTransaction request")
                  responseCallback(err)

                case Right((txnMetadata, newPreSendMetadata)) =>
                  // we can respond to the client immediately and continue to write the txn markers if
                  // the log append was successful
                  // 如果执行到这里，表示事务可以正常完成了，则会调用 responseCallback 返回成功响应给客户端。
                  // 生产者收到响应后就可以认为事务已经执行完成（但是还未完成，后续需要协调者来保证完成该事务）。
                  responseCallback(Errors.NONE)

                  // 接着调用 txnMarkerChannelManager#addTxnMarkersToSend 方法为该事务所有事务分区创建一个
                  // TxnIdAndMarkerEntry 实例，它表示一个待发送的 WriteTxnMarkers 请求，目标节点为分区Leader 副本。
                  // 然后添加到 transactionMarkerChannelManager#markersQueuePerBroker 中。
                  txnMarkerChannelManager.addTxnMarkersToSend(coordinatorEpoch, txnMarkerResult, txnMetadata, newPreSendMetadata)
              }
            } else {
              info(s"Aborting sending of transaction markers and returning $error error to client for $transactionalId's EndTransaction request of $txnMarkerResult, " +
                s"since appending $newMetadata to transaction log with coordinator epoch $coordinatorEpoch failed")

              if (isEpochFence) {
                txnManager.getTransactionState(transactionalId).foreach {
                  case None =>
                    warn(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
                      s"no metadata in the cache; this is not expected")

                  case Some(epochAndMetadata) =>
                    if (epochAndMetadata.coordinatorEpoch == coordinatorEpoch) {
                      // This was attempted epoch fence that failed, so mark this state on the metadata
                      epochAndMetadata.transactionMetadata.hasFailedEpochFence = true
                      warn(s"The coordinator failed to write an epoch fence transition for producer $transactionalId to the transaction log " +
                        s"with error $error. The epoch was increased to ${newMetadata.producerEpoch} but not returned to the client")
                    }
                }
              }

              responseCallback(error)
            }
          }

          // 将协调者事务状态切换到 PrepareCommit 状态，并将事务的 PrepareCommit 状态写入事务状态主题中，
          // 写入成功后会调用 sendTxnMarkersCallback 回调函数执行后续逻辑
          txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata, sendTxnMarkersCallback)
      }
    }
  }

  def transactionTopicConfigs: Properties = txnManager.transactionTopicConfigs

  def partitionFor(transactionalId: String): Int = txnManager.partitionFor(transactionalId)

  private def onEndTransactionComplete(txnIdAndPidEpoch: TransactionalIdAndProducerIdEpoch)(error: Errors): Unit = {
    error match {
      case Errors.NONE =>
        info("Completed rollback of ongoing transaction for transactionalId " +
          s"${txnIdAndPidEpoch.transactionalId} due to timeout")

      case error@(Errors.INVALID_PRODUCER_ID_MAPPING |
                  Errors.PRODUCER_FENCED |
                  Errors.CONCURRENT_TRANSACTIONS) =>
        debug(s"Rollback of ongoing transaction for transactionalId ${txnIdAndPidEpoch.transactionalId} " +
          s"has been cancelled due to error $error")

      case error =>
        warn(s"Rollback of ongoing transaction for transactionalId ${txnIdAndPidEpoch.transactionalId} " +
          s"failed due to error $error")
    }
  }

  private[transaction] def abortTimedOutTransactions(onComplete: TransactionalIdAndProducerIdEpoch => EndTxnCallback): Unit = {

    txnManager.timedOutTransactions().foreach { txnIdAndPidEpoch =>
      txnManager.getTransactionState(txnIdAndPidEpoch.transactionalId).foreach {
        case None =>
          error(s"Could not find transaction metadata when trying to timeout transaction for $txnIdAndPidEpoch")

        case Some(epochAndTxnMetadata) =>
          val txnMetadata = epochAndTxnMetadata.transactionMetadata
          val transitMetadataOpt = txnMetadata.inLock {
            if (txnMetadata.producerId != txnIdAndPidEpoch.producerId) {
              error(s"Found incorrect producerId when expiring transactionalId: ${txnIdAndPidEpoch.transactionalId}. " +
                s"Expected producerId: ${txnIdAndPidEpoch.producerId}. Found producerId: " +
                s"${txnMetadata.producerId}")
              None
            } else if (txnMetadata.pendingTransitionInProgress) {
              debug(s"Skipping abort of timed out transaction $txnIdAndPidEpoch since there is a " +
                "pending state transition")
              None
            } else {
              Some(txnMetadata.prepareFenceProducerEpoch())
            }
          }

          transitMetadataOpt.foreach { txnTransitMetadata =>
            endTransaction(txnMetadata.transactionalId,
              txnTransitMetadata.producerId,
              txnTransitMetadata.producerEpoch,
              TransactionResult.ABORT,
              isFromClient = false,
              onComplete(txnIdAndPidEpoch))
          }
      }
    }
  }

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableTransactionalIdExpiration: Boolean = true): Unit = {
    info("Starting up.")
    scheduler.startup()
    scheduler.schedule("transaction-abort",
      () => abortTimedOutTransactions(onEndTransactionComplete),
      txnConfig.abortTimedOutTransactionsIntervalMs,
      txnConfig.abortTimedOutTransactionsIntervalMs
    )
    if (enableTransactionalIdExpiration)
      txnManager.enableTransactionalIdExpiration()
    txnMarkerChannelManager.start()
    isActive.set(true)

    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown(): Unit = {
    info("Shutting down.")
    isActive.set(false)
    scheduler.shutdown()
    producerIdManager.shutdown()
    txnManager.shutdown()
    txnMarkerChannelManager.shutdown()
    info("Shutdown complete.")
  }
}

case class InitProducerIdResult(producerId: Long, producerEpoch: Short, error: Errors)
