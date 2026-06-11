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
                                                pendingCompleteTxnAndMarkerEntries: java.util.List[PendingCompleteTxnAndMarkerEntry]) extends RequestCompletionHandler with Logging {

  this.logIdent = "[Transaction Marker Request Completion Handler " + brokerId + "]: "

  override def onComplete(response: ClientResponse): Unit = {
    val requestHeader = response.requestHeader
    val correlationId = requestHeader.correlationId
    if (response.wasDisconnected) {
      trace(s"Cancelled request with header $requestHeader due to node ${response.destination} being disconnected")

      for (pendingCompleteTxnAndMarker <- pendingCompleteTxnAndMarkerEntries.asScala) {
        val pendingCompleteTxn = pendingCompleteTxnAndMarker.pendingCompleteTxn
        val transactionalId = pendingCompleteTxn.transactionalId
        val txnMarker = pendingCompleteTxnAndMarker.txnMarkerEntry

        txnStateManager.getTransactionState(transactionalId) match {

          case Left(Errors.NOT_COORDINATOR) =>
            info(s"I am no longer the coordinator for $transactionalId; cancel sending transaction markers $txnMarker to the brokers")

            txnMarkerChannelManager.removeMarkersForTxn(pendingCompleteTxn)

          case Left(Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
            info(s"I am loading the transaction partition that contains $transactionalId which means the current markers have to be obsoleted; " +
              s"cancel sending transaction markers $txnMarker to the brokers")

            txnMarkerChannelManager.removeMarkersForTxn(pendingCompleteTxn)

          case Left(unexpectedError) =>
            throw new IllegalStateException(s"Unhandled error $unexpectedError when fetching current transaction state")

          case Right(None) =>
            throw new IllegalStateException(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
              s"no metadata in the cache; this is not expected")

          case Right(Some(epochAndMetadata)) =>
            if (epochAndMetadata.coordinatorEpoch != txnMarker.coordinatorEpoch) {
              // coordinator epoch has changed, just cancel it from the purgatory
              info(s"Transaction coordinator epoch for $transactionalId has changed from ${txnMarker.coordinatorEpoch} to " +
                s"${epochAndMetadata.coordinatorEpoch}; cancel sending transaction markers $txnMarker to the brokers")

              txnMarkerChannelManager.removeMarkersForTxn(pendingCompleteTxn)
            } else {
              // re-enqueue the markers with possibly new destination brokers
              trace(s"Re-enqueuing ${txnMarker.transactionResult} transaction markers for transactional id $transactionalId " +
                s"under coordinator epoch ${txnMarker.coordinatorEpoch}")

              txnMarkerChannelManager.addTxnMarkersToBrokerQueue(txnMarker.producerId,
                txnMarker.producerEpoch,
                txnMarker.transactionResult,
                pendingCompleteTxn,
                txnMarker.partitions.asScala.toSet)
            }
        }
      }
    } else {
      debug(s"Received WriteTxnMarker response $response from node ${response.destination} with correlation id $correlationId")

      val writeTxnMarkerResponse = response.responseBody.asInstanceOf[WriteTxnMarkersResponse]

      val responseErrors = writeTxnMarkerResponse.errorsByProducerId
      for (pendingCompleteTxnAndMarker <- pendingCompleteTxnAndMarkerEntries.asScala) {
        val pendingCompleteTxn = pendingCompleteTxnAndMarker.pendingCompleteTxn
        val transactionalId = pendingCompleteTxn.transactionalId
        val txnMarker = pendingCompleteTxnAndMarker.txnMarkerEntry
        val errors = responseErrors.get(txnMarker.producerId)

        if (errors == null)
          throw new IllegalStateException(s"WriteTxnMarkerResponse does not contain expected error map for producer id ${txnMarker.producerId}")

        txnStateManager.getTransactionState(transactionalId) match {
          case Left(Errors.NOT_COORDINATOR) =>
            info(s"I am no longer the coordinator for $transactionalId; cancel sending transaction markers $txnMarker to the brokers")

            txnMarkerChannelManager.removeMarkersForTxn(pendingCompleteTxn)

          case Left(Errors.COORDINATOR_LOAD_IN_PROGRESS) =>
            info(s"I am loading the transaction partition that contains $transactionalId which means the current markers have to be obsoleted; " +
              s"cancel sending transaction markers $txnMarker to the brokers")

            txnMarkerChannelManager.removeMarkersForTxn(pendingCompleteTxn)

          case Left(unexpectedError) =>
            throw new IllegalStateException(s"Unhandled error $unexpectedError when fetching current transaction state")

          case Right(None) =>
            throw new IllegalStateException(s"The coordinator still owns the transaction partition for $transactionalId, but there is " +
              s"no metadata in the cache; this is not expected")

          case Right(Some(epochAndMetadata)) =>
            val txnMetadata = epochAndMetadata.transactionMetadata
            val retryPartitions: mutable.Set[TopicPartition] = mutable.Set.empty[TopicPartition]
            var abortSending: Boolean = false

            if (epochAndMetadata.coordinatorEpoch != txnMarker.coordinatorEpoch) {
              // coordinator epoch has changed, just cancel it from the purgatory
              info(s"Transaction coordinator epoch for $transactionalId has changed from ${txnMarker.coordinatorEpoch} to " +
                s"${epochAndMetadata.coordinatorEpoch}; cancel sending transaction markers $txnMarker to the brokers")

              txnMarkerChannelManager.removeMarkersForTxn(pendingCompleteTxn)
              abortSending = true
            } else {
              txnMetadata.inLock(() => {
                for ((topicPartition, error) <- errors.asScala) {
                  error match {
                    case Errors.NONE =>
                      // 성공: 해당 파티션의 TxnMarker 기록 완료 → 추적 목록에서 제거.
                      // 모든 파티션이 제거되면 maybeWriteTxnCompletion()이 COMPLETE 상태를 기록한다.
                      txnMetadata.removePartition(topicPartition)

                    case Errors.CORRUPT_MESSAGE |
                         Errors.MESSAGE_TOO_LARGE |
                         Errors.RECORD_LIST_TOO_LARGE |
                         Errors.INVALID_REQUIRED_ACKS =>
                      // 브로커가 정상이라면 절대 발생하지 않아야 하는 치명적 에러.
                      // 프로그래밍 버그이므로 IllegalStateException으로 크래시.
                      throw new IllegalStateException(s"Received fatal error ${error.exceptionName} while sending txn marker for $transactionalId")

                    case Errors.UNKNOWN_TOPIC_OR_PARTITION |
                         Errors.NOT_LEADER_OR_FOLLOWER |
                         Errors.NOT_ENOUGH_REPLICAS |
                         Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND |
                         Errors.REQUEST_TIMED_OUT |
                         Errors.KAFKA_STORAGE_ERROR =>
                      // 일시적 에러(리더 변경, 네트워크 타임아웃 등): 재시도 대상에 추가.
                      // 이후 addTxnMarkersToBrokerQueue()로 새 리더에게 재전송한다.
                      info(s"Sending $transactionalId's transaction marker for partition $topicPartition has failed with error ${error.exceptionName}, retrying " +
                        s"with current coordinator epoch ${epochAndMetadata.coordinatorEpoch}")

                      retryPartitions += topicPartition

                    case Errors.INVALID_PRODUCER_EPOCH |
                         Errors.TRANSACTION_COORDINATOR_FENCED =>
                      // Epoch가 변경되어 이 트랜잭션은 이미 stale 상태.
                      // 새 coordinator(또는 새 producer epoch)가 처리를 이어받으므로 마커 전송을 중단한다.
                      info(s"Sending $transactionalId's transaction marker for partition $topicPartition has permanently failed with error ${error.exceptionName} " +
                        s"with the current coordinator epoch ${epochAndMetadata.coordinatorEpoch}; cancel sending any more transaction markers $txnMarker to the brokers")

                      txnMarkerChannelManager.removeMarkersForTxn(pendingCompleteTxn)
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
              })
            }

            if (!abortSending) {
              if (retryPartitions.nonEmpty) {
                debug(s"Re-enqueuing ${txnMarker.transactionResult} transaction markers for transactional id $transactionalId " +
                  s"under coordinator epoch ${txnMarker.coordinatorEpoch}")

                // 재시도 파티션: 리더가 변경되었을 수 있으므로 새 리더를 조회해 큐에 재삽입한다.
                txnMarkerChannelManager.addTxnMarkersToBrokerQueue(
                  txnMarker.producerId,
                  txnMarker.producerEpoch,
                  txnMarker.transactionResult,
                  pendingCompleteTxn,
                  retryPartitions.toSet)
              } else {
                // 모든 파티션 성공: topicPartitions가 비었는지 확인 후 COMPLETE 상태를 __transaction_state에 기록한다.
                txnMarkerChannelManager.maybeWriteTxnCompletion(transactionalId)
              }
            }
        }
      }
    }
  }
}
