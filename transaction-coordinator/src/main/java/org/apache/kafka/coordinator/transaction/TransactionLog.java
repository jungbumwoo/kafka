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
package org.apache.kafka.coordinator.transaction;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.coordinator.transaction.generated.CoordinatorRecordType;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogKey;
import org.apache.kafka.coordinator.transaction.generated.TransactionLogValue;
import org.apache.kafka.server.common.TransactionVersion;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Messages stored for the transaction topic represent the producer id and transactional status of the corresponding
 * transactional id, which have versions for both the key and value fields. Key and value
 * versions are used to evolve the message formats:
 *
 * key version 0:               [transactionalId]
 *    -> value version 0:       [producer_id, producer_epoch, expire_timestamp, status, [topic, [partition] ], timestamp]
 *
 * <p>__transaction_state 토픽의 레코드 형식을 직렬화/역직렬화하는 유틸리티 클래스.
 *
 * <p>레코드 구조:
 * <pre>
 *   Key   = [2바이트 CoordinatorRecordType(=0)] + [TransactionLogKey: transactionalId 문자열]
 *   Value = [2바이트 version] + [TransactionLogValue 필드들]
 *
 *   Value 필드 (TransactionLogValue):
 *     - ProducerId               (int64)  : 현재 Producer ID (PID)
 *     - PreviousProducerId       (int64)  : 마지막으로 커밋된 트랜잭션의 Producer ID (v1+)
 *     - NextProducerId           (int64)  : Coordinator가 발급한 최신 Producer ID (v1+, epoch overflow 시 사용)
 *     - ProducerEpoch            (int16)  : 현재 epoch (Producer fencing에 사용)
 *     - NextProducerEpoch        (int16)  : NextProducerId에 대응하는 epoch (v1+)
 *     - TransactionTimeoutMs     (int32)  : 트랜잭션 타임아웃 (ms)
 *     - TransactionStatus        (int8)   : 상태 byte (TransactionState.id() 참조)
 *     - TransactionPartitions    (array)  : 이 트랜잭션에 참여 중인 topic-partition 목록
 *                                           EMPTY 상태일 때는 null(빈 배열)로 기록
 *     - TransactionLastUpdateTimestampMs (int64): 마지막 상태 변경 시각
 *     - TransactionStartTimestampMs      (int64): 트랜잭션 시작 시각 (첫 AddPartitions 시각)
 *     - ClientTransactionVersion         (int16): 클라이언트가 사용한 Transaction 프로토콜 버전 (v1+)
 *
 *   Tombstone(삭제 마커): value가 null인 레코드 → transactionalId 만료 시 기록,
 *                         log compaction 시 해당 키의 모든 이전 레코드 제거
 * </pre>
 *
 * <p>강제 설정:
 *   - 압축 없음(NONE): 레코드를 있는 그대로 저장
 *   - requiredAcks=-1: 모든 ISR 복제 완료 후 ack → 데이터 손실 방지
 *   - cleanup.policy=compact: 같은 key(transactionalId)의 최신 레코드만 보관
 */
public class TransactionLog {

    // enforce always using
    //  1. cleanup policy = compact
    //  2. compression = none
    //  3. unclean leader election = disabled
    //  4. required acks = -1 when writing
    // __transaction_state 쓰기 시 압축 사용 안 함 (레코드 크기가 작고 데이터 정합성 중요)
    public static final Compression ENFORCED_COMPRESSION = Compression.NONE;
    // 모든 ISR 복제가 완료되어야 ack를 반환 → Coordinator 장애 시 데이터 유실 방지
    public static final short ENFORCED_REQUIRED_ACKS = (short) -1;

    /**
     * Generates the bytes for transaction log message key
     *
     * @return key bytes
     *
     * <p>__transaction_state 레코드의 키를 직렬화한다.
     * 형식: [2바이트 CoordinatorRecordType.TRANSACTION_LOG.id() = 0] + [transactionalId UTF-8 문자열]
     * 이 키가 log compaction의 기준이 되어 같은 transactionalId의 최신 레코드만 보존된다.
     */
    public static byte[] keyToBytes(String transactionalId) {
        return MessageUtil.toCoordinatorTypePrefixedBytes(
                new TransactionLogKey().setTransactionalId(transactionalId)
        );
    }

    /**
     * Generates the payload bytes for transaction log message value
     *
     * @return value payload bytes
     *
     * <p>__transaction_state 레코드의 value를 직렬화한다.
     * 형식: [2바이트 valueVersion] + [TransactionLogValue 직렬화 바이트]
     *
     * <p>주요 동작:
     * 1. 상태가 EMPTY인 경우 topicPartitions를 null로 설정한다.
     *    EMPTY는 트랜잭션이 시작되지 않은 상태이므로 참여 파티션이 없다.
     * 2. 그 외 상태(ONGOING, PREPARE_COMMIT 등)에서는 현재 참여 중인
     *    topic-partition 목록을 topic 단위로 그룹화해 직렬화한다.
     * 3. TransactionVersion에 따라 value 버전(0 또는 1)이 결정된다.
     *    버전 1(TV2)에는 PreviousProducerId, NextProducerId, ClientTransactionVersion 등
     *    추가 필드가 포함된다.
     */
    public static byte[] valueToBytes(TxnTransitMetadata txnMetadata,
                                      TransactionVersion transactionVersionLevel) {
        // EMPTY 상태인데 파티션이 있으면 논리적 오류 → 예외를 던진다.
        if (txnMetadata.txnState() == TransactionState.EMPTY && !txnMetadata.topicPartitions().isEmpty()) {
            throw new IllegalStateException("Transaction is not expected to have any partitions since its state is "
                    + txnMetadata.txnState() + ": " + txnMetadata);
        }

        // EMPTY 상태이면 topicPartitions를 null로 설정 (직렬화 시 빈 배열로 처리됨).
        // EMPTY가 아닌 상태(ONGOING 등)에서는 참여 파티션을 topic 별로 그룹화해 직렬화한다.
        List<TransactionLogValue.PartitionsSchema> transactionPartitions = null;

        if (txnMetadata.txnState() != TransactionState.EMPTY) {
            transactionPartitions = txnMetadata.topicPartitions().stream()
                    .collect(Collectors.groupingBy(TopicPartition::topic))
                    .entrySet().stream()
                    .map(entry ->
                        new TransactionLogValue.PartitionsSchema().setTopic(entry.getKey())
                            .setPartitionIds(entry.getValue().stream().map(TopicPartition::partition).toList())).toList();
        }

        return MessageUtil.toVersionPrefixedBytes(
                transactionVersionLevel.transactionLogValueVersion(),
                new TransactionLogValue()
                        .setProducerId(txnMetadata.producerId())
                        .setProducerEpoch(txnMetadata.producerEpoch())
                        .setTransactionTimeoutMs(txnMetadata.txnTimeoutMs())
                        .setTransactionStatus(txnMetadata.txnState().id())
                        .setTransactionLastUpdateTimestampMs(txnMetadata.txnLastUpdateTimestamp())
                        .setTransactionStartTimestampMs(txnMetadata.txnStartTimestamp())
                        .setTransactionPartitions(transactionPartitions)
                        .setClientTransactionVersion(txnMetadata.clientTransactionVersion().featureLevel())
        );
    }

    /**
     * Decodes the transaction log messages' key
     *
     * @return the transactional id
     * @throws IllegalStateException if the version is not a valid transaction log key version
     */
    public static String readTxnRecordKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version == CoordinatorRecordType.TRANSACTION_LOG.id()) {
            return new TransactionLogKey(new ByteBufferAccessor(buffer), (short) 0).transactionalId();
        } else {
            throw new IllegalStateException("Unknown version " + version + " from the transaction log message key");
        }
    }



    public sealed interface ReadResult permits TxnRecord, TxnTombstone, UnknownKeyVersion, UnknownValueVersion { }

    // 정상 레코드: transactionalId와 역직렬화된 TransactionMetadata를 담는다.
    public record TxnRecord(String transactionId, TransactionMetadata metadata) implements ReadResult { }

    // 툼스톤 레코드: value가 null → 해당 transactionalId가 만료/삭제됨을 의미.
    // log compaction이 이 키에 대한 이전 레코드를 모두 제거한다.
    public record TxnTombstone(String transactionId) implements ReadResult { }

    // 미래 버전 호환성: 알 수 없는 키 버전이면 경고 후 무시
    public record UnknownKeyVersion(short version) implements ReadResult { }

    // 미래 버전 호환성: 알 수 없는 value 버전이면 경고 후 무시
    public record UnknownValueVersion(short version) implements ReadResult { }

    /**
     * Decodes the transaction log messages' key and value, returning a structured result.
     *
     * @return a {@link ReadResult} which is one of:
     *         <ul>
     *           <li>{@link TxnRecord} - contains the transactional id and metadata if successfully decoded</li>
     *           <li>{@link TxnTombstone} - if the value is null (tombstone record)</li>
     *           <li>{@link UnknownKeyVersion} - if the key version is not recognized</li>
     *           <li>{@link UnknownValueVersion} - if the value version is not recognized</li>
     *         </ul>
     *
     * <p>__transaction_state에서 읽어온 raw ByteBuffer를 역직렬화한다.
     * TransactionStateManager.loadTransactionMetadata()에서 Coordinator가 리더로 선출될 때
     * 파티션의 처음부터 끝까지 순서대로 읽으며 인메모리 캐시를 재구성한다.
     *
     * <p>처리 흐름:
     * 1. key 앞 2바이트로 CoordinatorRecordType 확인 → 알 수 없으면 UnknownKeyVersion 반환
     * 2. value가 null이면 TxnTombstone 반환 → 캐시에서 해당 transactionalId를 제거
     * 3. value 앞 2바이트로 valueVersion 확인 → 지원 범위 밖이면 UnknownValueVersion 반환
     * 4. 정상 value를 파싱해 TransactionMetadata 객체를 생성하고 TxnRecord로 반환
     *    - EMPTY 상태이면 topicPartitions를 빈 Set으로 초기화
     *    - 그 외 상태이면 PartitionsSchema 배열에서 TopicPartition Set을 복원
     */
    public static ReadResult read(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
        short keyVersion = keyBuffer.getShort();
        String transactionalId;
        if (keyVersion == CoordinatorRecordType.TRANSACTION_LOG.id()) {
            transactionalId = new TransactionLogKey(new ByteBufferAccessor(keyBuffer), (short) 0).transactionalId();
        } else {
            return new UnknownKeyVersion(keyVersion);
        }

        // value가 null이면 툼스톤 레코드 → 해당 transactionalId 캐시에서 제거
        if (valueBuffer == null) {
            return new TxnTombstone(transactionalId);
        } else {
            short valueVersion = valueBuffer.getShort();
            if (valueVersion >= TransactionLogValue.LOWEST_SUPPORTED_VERSION
                && valueVersion <= TransactionLogValue.HIGHEST_SUPPORTED_VERSION) {

                TransactionLogValue value = new TransactionLogValue(new ByteBufferAccessor(valueBuffer), valueVersion);
                // TransactionStatus 바이트를 TransactionState enum으로 변환
                TransactionState state = TransactionState.fromId(value.transactionStatus());

                // EMPTY 상태일 때는 파티션 목록이 null/빈 배열로 저장되어 있으므로 건너뜀
                Set<TopicPartition> tps = new HashSet<>();
                if (state != TransactionState.EMPTY) {
                    for (TransactionLogValue.PartitionsSchema partitionsSchema : value.transactionPartitions()) {
                        for (int partitionId : partitionsSchema.partitionIds()) {
                            tps.add(new TopicPartition(partitionsSchema.topic(), partitionId));
                        }
                    }
                }

                return new TxnRecord(transactionalId, new TransactionMetadata(
                    transactionalId,
                    value.producerId(),
                    value.previousProducerId(),
                    value.nextProducerId(),
                    value.producerEpoch(),
                    RecordBatch.NO_PRODUCER_EPOCH,
                    value.transactionTimeoutMs(),
                    state,
                    tps,
                    value.transactionStartTimestampMs(),
                    value.transactionLastUpdateTimestampMs(),
                    TransactionVersion.fromFeatureLevel(value.clientTransactionVersion()))
                );
            } else {
                return new UnknownValueVersion(valueVersion);
            }
        }
    }
}
