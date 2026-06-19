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

package org.apache.kafka.coordinator.group.classic;

import java.util.Locale;
import java.util.Set;

/**
 * Represents all states that a classic group can be in, as well as the states that a group must
 * be in to transition to a particular state.
 *
 * <p>Classic Rebalance Protocol 서버측 상태 머신 (stop-the-world 방식):
 *
 * <p>리밸런스 흐름과 상태 전환 매핑:
 * <pre>
 *   [SERVER-2]  prepareRebalance()         → 어떤 상태에서든 PREPARING_REBALANCE로 전환
 *   [SERVER-3a] completeClassicGroupJoin() → PREPARING_REBALANCE → COMPLETING_REBALANCE (initNextGeneration())
 *   [SERVER-6]  classicGroupSync() 리더처리 → COMPLETING_REBALANCE → STABLE (group.transitionTo(STABLE))
 * </pre>
 *
 * <pre>
 *                     new member joins / leader rejoins / metadata change
 *                              │
 *              ┌───────────────▼────────────────┐
 *              │       PREPARING_REBALANCE       │◄──────────────────────────────────────┐
 *              │  - 모든 멤버로부터 JoinGroup 대기  │                                       │
 *              │  - 하트비트에 REBALANCE_IN_PROGRESS│                                       │
 *              └───────────────┬────────────────┘                                       │
 *                              │ rebalanceTimeout 만료 또는 모든 멤버 합류                 │
 *              ┌───────────────▼────────────────┐                                       │
 *              │      COMPLETING_REBALANCE       │   new member / leave / heartbeat fail │
 *              │  - 리더로부터 SyncGroup(assignment)│ ──────────────────────────────────────
 *              │    대기; 팔로워 SyncGroup은 park  │
 *              └───────────────┬────────────────┘
 *                              │ 리더 SyncGroup 수신
 *              ┌───────────────▼────────────────┐
 *              │             STABLE              │
 *              │  - 정상 하트비트/소비 동작        │
 *              └───────────────┬────────────────┘
 *                              │ 모든 멤버 탈퇴
 *              ┌───────────────▼────────────────┐   모든 offset 만료
 *              │              EMPTY              │ ──────────────────► DEAD
 *              └────────────────────────────────┘
 * </pre>
 *
 * <p>상태 전환은 {@link #validPreviousStates}로 강제된다.
 * 잘못된 전환 시도는 {@link org.apache.kafka.coordinator.group.classic.ClassicGroup#transitionTo}에서 예외를 발생시킨다.
 */
public enum ClassicGroupState {

    /**
     * Group has no more members, but lingers until all offsets have expired. This state
     * also represents groups which use Kafka only for offset commits and have no members.
     *
     * action: respond normally to join group from new members
     *         respond to sync group with UNKNOWN_MEMBER_ID
     *         respond to heartbeat with UNKNOWN_MEMBER_ID
     *         respond to leave group with UNKNOWN_MEMBER_ID
     *         respond to offset commit with UNKNOWN_MEMBER_ID
     *         allow offset fetch requests
     * transition: last offsets removed in periodic expiration task => DEAD
     *             join group from a new member => PREPARING_REBALANCE
     *             group is removed by partition emigration => DEAD
     *             group is removed by expiration => DEAD
     *
     * <p>멤버가 없지만 오프셋 커밋만 사용하는 그룹도 이 상태로 시작한다.
     * 오프셋이 모두 만료될 때까지 유지되다가 DEAD로 전환된다.
     */
    EMPTY("Empty"),

    /**
     * Group is preparing to rebalance.
     *
     * action: respond to heartbeats with REBALANCE_IN_PROGRESS
     *         respond to sync group with REBALANCE_IN_PROGRESS
     *         remove member on leave group request
     *         park join group requests from new or existing members until all expected members have joined
     *         allow offset commits from previous generation
     *         allow offset fetch requests
     * transition: some members have joined by the timeout => COMPLETING_REBALANCE
     *             all members have left the group => EMPTY
     *             group is removed by partition emigration => DEAD
     *
     * <p>리밸런싱 시작 단계. 코디네이터는 rebalanceTimeout 동안 모든 멤버의 JoinGroup
     * 요청을 수집(park)한다. 기존 멤버가 하트비트를 보내면 REBALANCE_IN_PROGRESS를 받아
     * 다음 폴 루프에서 JoinGroup을 재전송한다. 이 상태가 "stop-the-world"의 핵심이다.
     */
    PREPARING_REBALANCE("PreparingRebalance"),

    /**
     * Group is awaiting state assignment from the leader.
     *
     * action: respond to heartbeats with REBALANCE_IN_PROGRESS
     *         respond to offset commits with REBALANCE_IN_PROGRESS
     *         park sync group requests from followers until transition to STABLE
     *         allow offset fetch requests
     * transition: sync group with state assignment received from leader => STABLE
     *             join group from new member or existing member with updated metadata => PREPARING_REBALANCE
     *             leave group from existing member => PREPARING_REBALANCE
     *             member failure detected => PREPARING_REBALANCE
     *             group is removed by partition emigration => DEAD
     *
     * <p>모든 멤버가 JoinGroup에 합류한 후의 단계. 코디네이터가 리더 멤버를 선출하고
     * 전체 멤버 목록을 JoinGroup 응답으로 돌려준다. 리더는 클라이언트 측에서 파티션
     * 할당 알고리즘을 수행한 뒤 SyncGroup으로 결과를 전송한다. 팔로워의 SyncGroup은
     * 리더 SyncGroup이 도착할 때까지 park된다.
     */
    COMPLETING_REBALANCE("CompletingRebalance"),

    /**
     * Group is stable.
     *
     * action: respond to member heartbeats normally
     *         respond to sync group from any member with current assignment
     *         respond to join group from followers with matching metadata with current group metadata
     *         allow offset commits from member of current generation
     *         allow offset fetch requests
     * transition: member failure detected via heartbeat => PREPARING_REBALANCE
     *             leave group from existing member => PREPARING_REBALANCE
     *             leader join-group received => PREPARING_REBALANCE
     *             follower join-group with new metadata => PREPARING_REBALANCE
     *             group is removed by partition emigration => DEAD
     *
     * <p>파티션 할당이 완료되어 정상 소비 중인 상태. 멤버 탈퇴/장애·리더 재참여·메타데이터
     * 변경이 감지되면 즉시 PREPARING_REBALANCE로 전환된다.
     */
    STABLE("Stable"),

    /**
     * Group has no more members and its metadata is being removed.
     *
     * action: respond to join group with UNKNOWN_MEMBER_ID
     *         respond to sync group with UNKNOWN_MEMBER_ID
     *         respond to heartbeat with UNKNOWN_MEMBER_ID
     *         respond to leave group with UNKNOWN_MEMBER_ID
     *         respond to offset commit with UNKNOWN_MEMBER_ID
     *         allow offset fetch requests
     * transition: DEAD is a final state before group metadata is cleaned up, so there are no transitions
     */
    DEAD("Dead");

    private final String name;
    private final String lowerCaseName;
    private Set<ClassicGroupState> validPreviousStates;

    static {
        EMPTY.addValidPreviousStates(PREPARING_REBALANCE);
        PREPARING_REBALANCE.addValidPreviousStates(STABLE, COMPLETING_REBALANCE, EMPTY);
        COMPLETING_REBALANCE.addValidPreviousStates(PREPARING_REBALANCE);
        STABLE.addValidPreviousStates(COMPLETING_REBALANCE);
        DEAD.addValidPreviousStates(STABLE, PREPARING_REBALANCE, COMPLETING_REBALANCE, EMPTY, DEAD);
    }

    ClassicGroupState(String name) {
        this.name = name;
        this.lowerCaseName = name.toLowerCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return name;
    }

    public String toLowerCaseString() {
        return lowerCaseName;
    }

    private void addValidPreviousStates(ClassicGroupState... validPreviousStates) {
        this.validPreviousStates = Set.of(validPreviousStates);
    }

    /**
     * @return valid previous states a group must be in to transition to this state.
     */
    public Set<ClassicGroupState> validPreviousStates() {
        return this.validPreviousStates;
    }
}
