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

package org.apache.kafka.controller;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.ProducerIdsBlock;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineLong;
import org.apache.kafka.timeline.TimelineObject;

import org.slf4j.Logger;

import java.util.List;


/**
 * KRaft 컨트롤러의 단일 스레드 이벤트 루프 안에서 실행되는 producer ID 블록 발급 관리자.
 *
 * <p><b>상태 관리:</b>
 * {@code nextProducerBlock}은 {@link TimelineObject}로 감싸져 Raft 스냅샷 오프셋 기준으로
 * 과거 시점의 값을 정확히 읽을 수 있다 (snapshot-safe). 초기값은 {@link ProducerIdsBlock#EMPTY}이며,
 * 첫 번째 {@code ProducerIdsRecord}가 replay되기 전까지 firstProducerId는 0이다.
 *
 * <p><b>발급 흐름 (2-phase write):</b>
 * <ol>
 *   <li>{@link #generateNextProducerId}: 현재 {@code nextProducerBlock}을 읽어 브로커에 줄 블록을 결정하고,
 *       {@code ProducerIdsRecord}(nextProducerId = block.nextBlockFirstId())를 Raft 로그에 기록할
 *       {@link ControllerResult}로 반환한다. 이 시점에는 아직 상태가 변경되지 않는다.</li>
 *   <li>{@link #replay}: Raft 로그 commit 후 호출된다. {@code nextProducerBlock}을 실제로 전진시켜
 *       다음 요청이 새 범위에서 시작하도록 한다. 단조 증가 위반 시 RuntimeException으로 크래시한다.</li>
 * </ol>
 */
public class ProducerIdControlManager {
    static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private ClusterControlManager clusterControlManager = null;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setClusterControlManager(ClusterControlManager clusterControlManager) {
            this.clusterControlManager = clusterControlManager;
            return this;
        }

        ProducerIdControlManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (clusterControlManager == null) {
                throw new RuntimeException("You must specify ClusterControlManager.");
            }
            return new ProducerIdControlManager(
                logContext,
                clusterControlManager,
                snapshotRegistry);
        }
    }

    private final Logger log;
    private final ClusterControlManager clusterControlManager;
    private final TimelineObject<ProducerIdsBlock> nextProducerBlock;
    private final TimelineLong brokerEpoch;

    private ProducerIdControlManager(
        LogContext logContext,
        ClusterControlManager clusterControlManager,
        SnapshotRegistry snapshotRegistry
    ) {
        this.log = logContext.logger(ProducerIdControlManager.class);
        this.clusterControlManager = clusterControlManager;
        this.nextProducerBlock = new TimelineObject<>(snapshotRegistry, ProducerIdsBlock.EMPTY);
        this.brokerEpoch = new TimelineLong(snapshotRegistry);
    }

    /**
     * 요청 브로커에게 발급할 producer ID 블록을 결정하고, Raft 로그에 기록할 레코드를 반환한다.
     *
     * <p>이 메서드는 상태를 직접 변경하지 않는다. 실제 {@code nextProducerBlock} 전진은
     * Raft commit 이후 {@link #replay}에서 이루어진다 (2-phase write).
     *
     * <p>처리 순서:
     * <ol>
     *   <li>{@code checkBrokerEpoch}: stale 브로커(구 epoch)의 요청을 거부한다.</li>
     *   <li>현재 {@code nextProducerBlock.firstProducerId()}를 블록 시작점으로 사용한다.
     *       Long 오버플로우 방지 체크 후 1,000개짜리 {@link ProducerIdsBlock}을 생성한다.</li>
     *   <li>{@code ProducerIdsRecord}에 {@code nextProducerId = block.nextBlockFirstId()}를 기록한다.
     *       이 값이 commit 후 replay()에서 다음 블록의 시작점이 된다.</li>
     *   <li>{@link ControllerResult}로 (Raft 레코드, 브로커에 반환할 블록)을 함께 묶어 반환한다.</li>
     * </ol>
     */
    ControllerResult<ProducerIdsBlock> generateNextProducerId(int brokerId, long brokerEpoch) {
        clusterControlManager.checkBrokerEpoch(brokerId, brokerEpoch);

        long firstProducerIdInBlock = nextProducerBlock.get().firstProducerId();
        if (firstProducerIdInBlock > Long.MAX_VALUE - ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE) {
            throw new UnknownServerException("Exhausted all producerIds as the next block's end producerId " +
                "has exceeded the int64 type limit");
        }

        ProducerIdsBlock block = new ProducerIdsBlock(brokerId, firstProducerIdInBlock, ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE);
        long newNextProducerId = block.nextBlockFirstId();

        ProducerIdsRecord record = new ProducerIdsRecord()
            .setNextProducerId(newNextProducerId)
            .setBrokerId(brokerId)
            .setBrokerEpoch(brokerEpoch);
        return ControllerResult.of(List.of(new ApiMessageAndVersion(record, (short) 0)), block);
    }

    // VisibleForTesting
    ProducerIdsBlock nextProducerBlock() {
        return nextProducerBlock.get();
    }

    /**
     * {@code ProducerIdsRecord}가 Raft 로그에 commit된 후 호출되어 컨트롤러 상태를 전진시킨다.
     *
     * <p>{@code nextProducerBlock}을 record에 담긴 {@code nextProducerId}에서 시작하는
     * 새 블록으로 교체한다. 이로써 다음 {@link #generateNextProducerId} 호출은 이 블록의
     * firstProducerId를 시작점으로 사용한다.
     *
     * <p>단조 증가 검증: record의 nextProducerId가 현재 블록의 firstProducerId 이하이면
     * ID가 퇴행하는 것이므로 RuntimeException으로 크래시한다.
     * (migration 중 EMPTY 상태일 때는 검증을 생략한다.)
     */
    void replay(ProducerIdsRecord record) {
        // During a migration, we may be calling replay() without ever having called generateNextProducerId(),
        // so the next producer block could be EMPTY
        ProducerIdsBlock nextBlock = nextProducerBlock.get();
        if (nextBlock != ProducerIdsBlock.EMPTY && record.nextProducerId() <= nextBlock.firstProducerId()) {
            throw new RuntimeException("Next Producer ID from replayed record (" + record.nextProducerId() + ")" +
                " is not greater than current next Producer ID in block (" + nextBlock + ")");
        } else {
            log.info("Replaying ProducerIdsRecord {}", record);
            nextProducerBlock.set(new ProducerIdsBlock(record.brokerId(), record.nextProducerId(),
                    ProducerIdsBlock.PRODUCER_ID_BLOCK_SIZE));
            brokerEpoch.set(record.brokerEpoch());
        }
    }
}
