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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents the states of a transaction in the transaction coordinator.
 * This enum corresponds to the Scala sealed trait TransactionState in kafka.coordinator.transaction.
 *
 * <p>__transaction_state 토픽에 저장되는 트랜잭션 상태를 나타내는 열거형.
 * 각 상태는 1바이트 ID로 직렬화되어 {@link TransactionLog#valueToBytes}를 통해
 * __transaction_state 토픽의 value 필드(TransactionStatus)에 기록된다.
 *
 * <p>전체 상태 전이 다이어그램:
 * <pre>
 *   [InitProducerId 요청]
 *   없음(최초) ──────────────────────────────────► EMPTY (id=0)
 *                                                     │
 *   [AddPartitionsToTxn / AddOffsetsToTxn 요청]       │
 *   COMPLETE_COMMIT ──────────────────────────────────┤
 *   COMPLETE_ABORT ───────────────────────────────────┤
 *                                                     ▼
 *                                                 ONGOING (id=1)
 *                                                 │       │
 *                        [EndTxn commit 요청]     │       │ [EndTxn abort 요청]
 *                                                 ▼       ▼
 *                                     PREPARE_COMMIT   PREPARE_ABORT (id=2,3)
 *                                          │                │
 *               [모든 파티션 TxnMarker ack] │                │ [모든 파티션 TxnMarker ack]
 *                                          ▼                ▼
 *                                   COMPLETE_COMMIT   COMPLETE_ABORT (id=4,5)
 *                                          │                │
 *                          [만료 허용]      └──────┬─────────┘
 *                                                 ▼
 *                                             DEAD (id=6)  [캐시에서 제거, 툼스톤 기록]
 *
 *   PREPARE_EPOCH_FENCE (id=7): 내부적으로 오래된 Producer를 fencing할 때 사용
 *                               ONGOING → PREPARE_EPOCH_FENCE → PREPARE_ABORT 순으로 전이
 * </pre>
 */
public enum TransactionState {
    /**
     * Transaction has not existed yet
     * <p>
     * transition: received AddPartitionsToTxnRequest => Ongoing
     *             received AddOffsetsToTxnRequest => Ongoing
     *             received EndTxnRequest with abort and TransactionV2 enabled => PrepareAbort
     *
     * <p>__transaction_state에 기록되는 내용:
     * TransactionStatus=0, topicPartitions=null(빈 배열),
     * txnStartTimestamp=-1, producerEpoch=새 epoch
     */
    EMPTY((byte) 0, org.apache.kafka.clients.admin.TransactionState.EMPTY.toString(), true),
    /**
     * Transaction has started and ongoing
     * <p>
     * transition: received EndTxnRequest with commit => PrepareCommit
     *             received EndTxnRequest with abort => PrepareAbort
     *             received AddPartitionsToTxnRequest => Ongoing
     *             received AddOffsetsToTxnRequest => Ongoing
     *
     * <p>__transaction_state에 기록되는 내용:
     * TransactionStatus=1, topicPartitions=[참여 파티션 목록],
     * txnStartTimestamp=최초 AddPartitions 시각
     */
    ONGOING((byte) 1, org.apache.kafka.clients.admin.TransactionState.ONGOING.toString(), false),
    /**
     * Group is preparing to commit
     * transition: received acks from all partitions => CompleteCommit
     *
     * <p>__transaction_state에 기록되는 내용:
     * TransactionStatus=2, topicPartitions=[참여 파티션 목록 유지]
     * 이 레코드가 기록된 후 TransactionMarkerChannelManager가 각 파티션 리더에게
     * WriteTxnMarkers(COMMIT) 요청을 전송한다.
     */
    PREPARE_COMMIT((byte) 2, org.apache.kafka.clients.admin.TransactionState.PREPARE_COMMIT.toString(), false),
    /**
     * Group is preparing to abort
     * <p>
     * transition: received acks from all partitions => CompleteAbort
     * <p>
     * Note, In transaction v2, we allow Empty, CompleteCommit, CompleteAbort to transition to PrepareAbort. because the
     * client may not know the txn state on the server side, it needs to send endTxn request when uncertain.
     *
     * <p>__transaction_state에 기록되는 내용:
     * TransactionStatus=3, topicPartitions=[참여 파티션 목록 유지]
     * 이 레코드가 기록된 후 TransactionMarkerChannelManager가 각 파티션 리더에게
     * WriteTxnMarkers(ABORT) 요청을 전송한다.
     */
    PREPARE_ABORT((byte) 3, org.apache.kafka.clients.admin.TransactionState.PREPARE_ABORT.toString(), false),
    /**
     * Group has completed commit
     * <p>
     * Will soon be removed from the ongoing transaction cache
     *
     * <p>__transaction_state에 기록되는 내용:
     * TransactionStatus=4, topicPartitions=[]
     * 모든 파티션에 TxnMarker(COMMIT) ack를 받은 후 기록된다.
     * 기록 후 캐시에서 제거되며, 다음 InitProducerId 요청 시 EMPTY → ONGOING으로 재사용된다.
     */
    COMPLETE_COMMIT((byte) 4, org.apache.kafka.clients.admin.TransactionState.COMPLETE_COMMIT.toString(), true),
    /**
     * Group has completed abort
     * <p>
     * Will soon be removed from the ongoing transaction cache
     *
     * <p>__transaction_state에 기록되는 내용:
     * TransactionStatus=5, topicPartitions=[]
     * 모든 파티션에 TxnMarker(ABORT) ack를 받은 후 기록된다.
     */
    COMPLETE_ABORT((byte) 5, org.apache.kafka.clients.admin.TransactionState.COMPLETE_ABORT.toString(), true),
    /**
     * TransactionalId has expired and is about to be removed from the transaction cache
     *
     * <p>__transaction_state에 기록되는 내용:
     * null value 레코드(툼스톤)가 기록되어 log compaction에 의해 해당 transactionalId의
     * 모든 이전 레코드가 정리된다. 캐시에서도 제거된다.
     */
    DEAD((byte) 6, "Dead", false),
    /**
     * We are in the middle of bumping the epoch and fencing out older producers.
     *
     * <p>내부 전용 상태. 오래된 Producer를 격리(fencing)하기 위해 epoch를 올리는 중간 상태.
     * __transaction_state에 기록 후 즉시 PREPARE_ABORT로 전이된다.
     * 클라이언트는 이 상태를 직접 볼 수 없다.
     */
    PREPARE_EPOCH_FENCE((byte) 7, org.apache.kafka.clients.admin.TransactionState.PREPARE_EPOCH_FENCE.toString(), false);

    private static final Map<String, TransactionState> NAME_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toUnmodifiableMap(TransactionState::stateName, Function.identity()));

    private static final Map<Byte, TransactionState> ID_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toUnmodifiableMap(TransactionState::id, Function.identity()));

    public static final Set<TransactionState> ALL_STATES = Set.copyOf(EnumSet.allOf(TransactionState.class));

    private final byte id;
    private final String stateName;

    /**
     * 각 상태로 전이하기 위해 허용된 이전 상태 집합.
     * prepareTransitionTo() 에서 이 맵을 조회해 유효하지 않은 전이를 차단한다.
     *
     * 요약:
     *   EMPTY           ← EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT  (InitProducerId 후 초기화)
     *   ONGOING         ← EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT, ONGOING  (AddPartitions)
     *   PREPARE_COMMIT  ← ONGOING   (EndTxn commit)
     *   PREPARE_ABORT   ← ONGOING, PREPARE_EPOCH_FENCE, EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT
     *                     (EndTxn abort; TV2에서는 Empty/Complete 상태에서도 허용)
     *   COMPLETE_COMMIT ← PREPARE_COMMIT  (TxnMarker ack 완료)
     *   COMPLETE_ABORT  ← PREPARE_ABORT   (TxnMarker ack 완료)
     *   DEAD            ← EMPTY, COMPLETE_ABORT, COMPLETE_COMMIT  (transactionalId 만료)
     *   PREPARE_EPOCH_FENCE ← ONGOING  (Producer fencing 중)
     */
    public static final Map<TransactionState, Set<TransactionState>> VALID_PREVIOUS_STATES = Map.of(
        EMPTY, Set.of(EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT),
        ONGOING, Set.of(ONGOING, EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT),
        PREPARE_COMMIT, Set.of(ONGOING),
        PREPARE_ABORT, Set.of(ONGOING, PREPARE_EPOCH_FENCE, EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT),
        COMPLETE_COMMIT, Set.of(PREPARE_COMMIT),
        COMPLETE_ABORT, Set.of(PREPARE_ABORT),
        DEAD, Set.of(EMPTY, COMPLETE_ABORT, COMPLETE_COMMIT),
        PREPARE_EPOCH_FENCE, Set.of(ONGOING)
    );

    private final boolean expirationAllowed;

    TransactionState(byte id, String name, boolean expirationAllowed) {
        this.id = id;
        this.stateName = name;
        this.expirationAllowed = expirationAllowed;
    }

    /**
     * @return The state id byte.
     */
    public byte id() {
        return id;
    }

    /**
     * Get the name of this state. This is exposed through the `DescribeTransactions` API.
     * @return The state name string.
     */
    public String stateName() {
        return stateName;
    }

    /**
     * @return The set of states from which it is valid to transition into this state.
     */
    public Set<TransactionState> validPreviousStates() {
        return VALID_PREVIOUS_STATES.getOrDefault(this, Set.of());
    }

    /**
     * @return True if expiration is allowed in this state, false otherwise.
     */
    public boolean isExpirationAllowed() {
        return expirationAllowed;
    }

    /**
     * Finds a TransactionState by its name.
     * @param name The name of the state.
     * @return An Optional containing the TransactionState if found, otherwise empty.
     */
    public static Optional<TransactionState> fromName(String name) {
        return Optional.ofNullable(NAME_TO_ENUM.get(name));
    }

    /**
     * Finds a TransactionState by its ID.
     * @param id The byte ID of the state.
     * @return The TransactionState corresponding to the ID.
     * @throws IllegalStateException if the ID is unknown.
     */
    public static TransactionState fromId(byte id) {
        TransactionState state = ID_TO_ENUM.get(id);
        if (state == null) {
            throw new IllegalStateException("Unknown transaction state id " + id + " from the transaction status message");
        }
        return state;
    }
}
