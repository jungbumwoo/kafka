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

package org.apache.kafka.server.common;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds a range of Producer IDs used for Transactional and EOS producers.
 * <br>
 * The start and end of the ID block are inclusive.
 *
 * <p><b>2-tier 발급 구조:</b>
 * <ul>
 *   <li><b>Controller tier</b>: 전체 producer ID 네임스페이스를 소유한다.
 *       브로커가 {@code AllocateProducerIds} 요청을 보내면 1,000개 단위 블록을 할당하고
 *       {@code ProducerIdsRecord}를 Raft 로그에 commit해 내구성을 보장한다.</li>
 *   <li><b>Broker tier</b>: 할당받은 블록에서 {@link #claimNextId()}로 ID를 하나씩 꺼낸다.
 *       90% 소진 시점에 다음 블록을 미리 prefetch해 대기 없이 연속 발급이 가능하다.</li>
 * </ul>
 *
 * <p>{@code producerIdCounter}는 {@link AtomicLong}이므로 여러 스레드가 동시에
 * {@link #claimNextId()}를 호출해도 중복 없이 단조 증가한다.
 */
public class ProducerIdsBlock {
    /**
     * 컨트롤러가 브로커에게 한 번에 발급하는 producer ID 블록 크기 (1,000개 고정).
     * 블록 단위 발급은 매 ID마다 컨트롤러로 RPC를 보내는 오버헤드를 제거한다.
     */
    public static final int PRODUCER_ID_BLOCK_SIZE = 1000;

    public static final ProducerIdsBlock EMPTY = new ProducerIdsBlock(-1, 0, 0);

    private final int assignedBrokerId;
    private final long firstProducerId;
    private final int blockSize;
    private final AtomicLong producerIdCounter;

    public ProducerIdsBlock(int assignedBrokerId, long firstProducerId, int blockSize) {
        this.assignedBrokerId = assignedBrokerId;
        this.firstProducerId = firstProducerId;
        this.blockSize = blockSize;
        producerIdCounter = new AtomicLong(firstProducerId);
    }

    /**
     * Claim the next available producer id from the block.
     * Returns an empty result if there are no more available producer ids in the block.
     *
     * <p>{@code AtomicLong.getAndIncrement()}으로 lock 없이 스레드-안전하게 다음 ID를 예약한다.
     * 반환값이 {@link #lastProducerId()}를 초과하면 블록이 소진된 것이므로 {@code Optional.empty()}를 돌려준다.
     * 이 경우 호출자({@code RPCProducerIdManager})는 prefetch해 둔 다음 블록으로 교체를 시도한다.
     */
    public Optional<Long> claimNextId() {
        long nextId = producerIdCounter.getAndIncrement();
        if (nextId > lastProducerId()) {
            return Optional.empty();
        }
        return Optional.of(nextId);
    }

    /**
     * Get the ID of the broker that this block was assigned to.
     */
    public int assignedBrokerId() {
        return assignedBrokerId;
    }

    /**
     * Get the first ID (inclusive) to be assigned from this block.
     */
    public long firstProducerId() {
        return firstProducerId;
    }

    /**
     * Get the number of IDs contained in this block.
     */
    public int size() {
        return blockSize;
    }

    /**
     * Get the last ID (inclusive) to be assigned from this block.
     */
    public long lastProducerId() {
        return firstProducerId + blockSize - 1;
    }

    /**
     * Get the first ID of the next block following this one.
     */
    public long nextBlockFirstId() {
        return firstProducerId + blockSize;
    }

    @Override
    public String toString() {
        return "ProducerIdsBlock(" +
                "assignedBrokerId=" + assignedBrokerId +
                ", firstProducerId=" + firstProducerId +
                ", size=" + blockSize +
                ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerIdsBlock that = (ProducerIdsBlock) o;
        return assignedBrokerId == that.assignedBrokerId && firstProducerId == that.firstProducerId && blockSize == that.blockSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignedBrokerId, firstProducerId, blockSize);
    }
}
