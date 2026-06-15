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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AllocateProducerIdsRequest;
import org.apache.kafka.common.requests.AllocateProducerIdsResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.ProducerIdsBlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * RPCProducerIdManager allocates producer id blocks asynchronously and will immediately fail requests
 * for producers to retry if it does not have an available producer id and is waiting on a new block.
 *
 * <p><b>브로커-side 블록 관리 전략:</b>
 * <ul>
 *   <li>current/next 두 블록 슬롯을 {@link AtomicReference}로 관리한다.</li>
 *   <li>currentBlock이 90% 소진되면 컨트롤러에 {@code AllocateProducerIds}를 비동기로 보내
 *       nextBlock을 미리 채운다 (prefetch). 정상 상황에서 브로커는 블록 교체 시 대기하지 않는다.</li>
 *   <li>currentBlock이 완전히 소진된 시점에 nextBlock도 없으면 {@code COORDINATOR_LOAD_IN_PROGRESS}를
 *       던져 클라이언트가 재시도하도록 한다 (구버전 클라이언트 호환 이유로 REQUEST_TIMED_OUT 대신 사용).</li>
 *   <li>인플라이트 요청은 {@code requestInFlight} CAS로 1개로 제한한다.</li>
 * </ul>
 */
public class RPCProducerIdManager implements ProducerIdManager {

    static final int RETRY_BACKOFF_MS = 50;
    // Once we reach this percentage of PIDs consumed from the current block, trigger a fetch of the next block
    // 현재 블록이 90% 소진되는 시점에 prefetch를 트리거한다.
    // 나머지 10%(100개)가 컨트롤러 응답 대기 시간의 여유분(buffer)이 된다.
    private static final double PID_PREFETCH_THRESHOLD = 0.90;
    // 블록 교체 후 재시도 횟수 상한. 정상적으로는 1회 교체로 충분하므로 3은 방어적 상한이다.
    private static final int ITERATION_LIMIT = 3;
    private static final long NO_RETRY = -1L;

    private static final Logger log = LoggerFactory.getLogger(RPCProducerIdManager.class);
    private final String logPrefix;

    private final int brokerId;
    // Visible for testing
    final Time time;
    private final Supplier<Long> brokerEpochSupplier;
    private final NodeToControllerChannelManager controllerChannel;

    // Visible for testing
    final AtomicReference<ProducerIdsBlock> nextProducerIdBlock = new AtomicReference<>(null);
    final AtomicReference<ProducerIdsBlock> currentProducerIdBlock = new AtomicReference<>(ProducerIdsBlock.EMPTY);
    private final AtomicBoolean requestInFlight = new AtomicBoolean(false);
    
    // Setting the value of backoffDeadlineMs should be handled only in the response handler thread.
    // Otherwise, consider using compareAndSet() instead of set().
    private final AtomicLong backoffDeadlineMs = new AtomicLong(NO_RETRY);

    public RPCProducerIdManager(int brokerId,
                                Time time,
                                Supplier<Long> brokerEpochSupplier,
                                NodeToControllerChannelManager controllerChannel
    ) {
        this.brokerId = brokerId;
        this.time = time;
        this.brokerEpochSupplier = brokerEpochSupplier;
        this.controllerChannel = controllerChannel;
        this.logPrefix = "[RPC ProducerId Manager " + brokerId + "]: ";
    }


    // jb 여기 발급 받는 방식 흥미로움. controller에서 id를 발급.
    // 발급 로직이 최적화되어있음. batch 단위로 미리 발급 받고, 특정 임계값에 도달하면 미리 block 단위로 발급 받아오도록 되어있다.
    @Override
    public long generateProducerId() {
        // 블록 교체가 필요한 경우 최대 ITERATION_LIMIT회까지 재시도한다.
        var iteration = 0;
        while (iteration <= ITERATION_LIMIT) {
            var claimNextId = currentProducerIdBlock.get().claimNextId();
            if (claimNextId.isPresent()) {
                long nextProducerId = claimNextId.get();
                // 현재 발급한 ID가 prefetch 기준점(90%)에 도달하면 다음 블록을 비동기로 요청한다.
                // nextProducerId == prefetchTarget 이면 정확히 한 번만 트리거된다.
                var prefetchTarget = currentProducerIdBlock.get().firstProducerId() +
                        (long) (currentProducerIdBlock.get().size() * PID_PREFETCH_THRESHOLD);
                if (nextProducerId == prefetchTarget) {
                    maybeRequestNextBlock();
                }
                return nextProducerId;
            } else {
                // Check the next block if current block is full
                // currentBlock 소진: nextBlock으로 교체를 시도한다.
                var block = nextProducerIdBlock.getAndSet(null);
                if (block == null) {
                    // nextBlock도 준비되지 않은 경우 — prefetch가 늦거나 컨트롤러 응답이 아직 안 온 상태.
                    // Return COORDINATOR_LOAD_IN_PROGRESS rather than REQUEST_TIMED_OUT since older clients treat the error as fatal
                    // when it should be retriable like COORDINATOR_LOAD_IN_PROGRESS.
                    maybeRequestNextBlock();
                    throw Errors.COORDINATOR_LOAD_IN_PROGRESS.exception("Producer ID block is full. Waiting for next block");
                } else {
                    // nextBlock을 currentBlock으로 승격하고 루프를 재시작한다.
                    currentProducerIdBlock.set(block);
                    requestInFlight.set(false);
                    iteration++;
                }
            }
        }
        throw Errors.COORDINATOR_LOAD_IN_PROGRESS.exception("Producer ID block is full. Waiting for next block");
    }

    /**
     * 다음 블록 요청이 필요한지 확인하고, 조건을 만족하면 {@link #sendRequest()}를 호출한다.
     *
     * <p>세 가지 조건이 모두 충족될 때만 요청을 보낸다:
     * <ol>
     *   <li>backoff 기간이 만료되었거나 설정되지 않았을 것 (에러 후 재시도 쿨다운 존중)</li>
     *   <li>nextProducerIdBlock이 아직 null일 것 (이미 다음 블록이 준비됐으면 불필요)</li>
     *   <li>{@code requestInFlight.compareAndSet(false, true)} 성공 (동시 중복 요청 방지)</li>
     * </ol>
     */
    private void maybeRequestNextBlock() {
        var retryTimestamp = backoffDeadlineMs.get();
        if (retryTimestamp == NO_RETRY || time.milliseconds() >= retryTimestamp) {
            // Send a request only if we reached the retry deadline, or if no deadline was set.
            if (nextProducerIdBlock.get() == null &&
                    requestInFlight.compareAndSet(false, true)) {
                sendRequest();
            }
        }
    }

    /**
     * 컨트롤러에 {@code AllocateProducerIds} 요청을 비동기로 전송한다.
     *
     * <p>요청에는 현재 brokerEpoch(live 값)와 brokerId를 담는다.
     * 컨트롤러는 brokerEpoch로 stale 브로커 요청을 거부한다.
     * 응답은 {@link #handleAllocateProducerIdsResponse}에서 처리되고,
     * 타임아웃 시 {@code backoffDeadlineMs}를 NO_RETRY로 재설정해 즉시 재시도가 가능하게 한다.
     */
    protected void sendRequest() {
        var message = new AllocateProducerIdsRequestData()
                .setBrokerEpoch(brokerEpochSupplier.get())
                .setBrokerId(brokerId);
        var request = new AllocateProducerIdsRequest.Builder(message);
        log.debug("{} Requesting next Producer ID block", logPrefix);
        controllerChannel.sendRequest(request, new ControllerRequestCompletionHandler() {

            @Override
            public void onComplete(ClientResponse response) {
                handleAllocateProducerIdsResponse(response);
            }

            @Override
            public void onTimeout() {
                log.warn("{} Timed out when requesting AllocateProducerIds from the controller.", logPrefix);
                backoffDeadlineMs.set(NO_RETRY);
                requestInFlight.set(false);
            }
        });
    }

    private void handleUnsuccessfulResponse() {
        // There is no need to compare and set because only one thread
        // handles the AllocateProducerIds response.
        backoffDeadlineMs.set(time.milliseconds() + RETRY_BACKOFF_MS);
        requestInFlight.set(false);
    }

    protected void handleAllocateProducerIdsResponse(ClientResponse clientResponse) {
        if (clientResponse.authenticationException() != null) {
            log.error("{} Unable to allocate producer id because of an authentication exception", logPrefix, clientResponse.authenticationException());
            handleUnsuccessfulResponse();
            return;
        }
        if (clientResponse.versionMismatch() != null) {
            log.error("{} Unable to allocate producer id because of a version mismatch exception", logPrefix, clientResponse.versionMismatch());
            handleUnsuccessfulResponse();
            return;
        }
        if (!clientResponse.hasResponse()) {
            log.error("{} Unable to allocate producer id because of empty response from controller", logPrefix);
            handleUnsuccessfulResponse();
            return;
        }
        AllocateProducerIdsResponse response = (AllocateProducerIdsResponse) clientResponse.responseBody();
        var data = response.data();
        var successfulResponse = false;
        var errors = Errors.forCode(data.errorCode());
        switch (errors) {
            case NONE:
                log.debug("{} Got next producer ID block from controller {}", logPrefix, data);
                successfulResponse = sanityCheckResponse(data);
                break;
            case STALE_BROKER_EPOCH:
                log.warn("{} Our broker currentBlockCount was stale, trying again.", logPrefix);
                break;
            case BROKER_ID_NOT_REGISTERED:
                log.warn("{} Our broker ID is not yet known by the controller, trying again.", logPrefix);
                break;
            default :
                log.error("{} Received error code {} from the controller.", logPrefix, errors);
        }
        if (!successfulResponse) {
            handleUnsuccessfulResponse();
        } else {
            backoffDeadlineMs.set(NO_RETRY);
        }
    }

    /**
     * 컨트롤러 응답의 유효성을 검사하고, 통과하면 다음 블록을 {@code nextProducerIdBlock}에 저장한다.
     *
     * <p>두 가지 불변식을 검증한다:
     * <ol>
     *   <li><b>단조 증가</b>: 새 블록의 firstId가 현재 블록의 lastId보다 커야 한다.
     *       그렇지 않으면 ID 중복 발급이 발생하므로 에러 로그 후 false를 반환한다.</li>
     *   <li><b>범위 유효성</b>: firstId와 blockLen이 음수이거나, 합산이 Long.MAX_VALUE를 넘으면 안 된다.</li>
     * </ol>
     * 검증 통과 시 {@code nextProducerIdBlock}에 새 블록을 세팅한다.
     * {@code generateProducerId()}가 currentBlock 소진을 감지하면 이 블록을 current로 승격한다.
     */
    private boolean sanityCheckResponse(AllocateProducerIdsResponseData data) {
        if (data.producerIdStart() <= currentProducerIdBlock.get().lastProducerId()) {
            log.error("{} Producer ID block is not monotonic with current block: current={} response={}", logPrefix, currentProducerIdBlock.get(), data);
        } else if (data.producerIdStart() < 0 || data.producerIdLen() < 0 || data.producerIdStart() > Long.MAX_VALUE - data.producerIdLen()) {
            log.error("{} Producer ID block includes invalid ID range: {}", logPrefix, data);
        } else {
            nextProducerIdBlock.set(new ProducerIdsBlock(brokerId, data.producerIdStart(), data.producerIdLen()));
            return true;
        }
        return false;
    }
}
