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
package org.apache.kafka.coordinator.group.modern.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.TopicIds;
import org.apache.kafka.coordinator.group.modern.UnionSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * The CurrentAssignmentBuilder class encapsulates the reconciliation engine of the
 * consumer group protocol. Given the current state of a member and a desired or target
 * assignment state, the state machine takes the necessary steps to converge them.
 *
 * <p>KIP-848 Consumer Protocol 멤버 상태 머신 (서버 측 점진적 조정):
 * Classic 프로토콜의 stop-the-world 방식과 달리 각 멤버가 독립적으로 조정된다.
 *
 * <pre>
 *  ┌──────────────────────────────────────────────────────────────────────────┐
 *  │  ConsumerGroupHeartbeat 수신 시 매번 build()가 호출되어 멤버 상태를 전진시킨다  │
 *  └──────────────────────────────────────────────────────────────────────────┘
 *
 *  [멤버 상태 전환]
 *
 *  STABLE ──────────────► (새 targetAssignment 존재)
 *                              └──► computeNextAssignment()
 *                                        ├─ revoke 필요 → UNREVOKED_PARTITIONS
 *                                        ├─ assign 가능 → STABLE (또는 UNRELEASED_PARTITIONS)
 *                                        └─ 대기 중인 파티션만 있음 → UNRELEASED_PARTITIONS
 *
 *  UNREVOKED_PARTITIONS ─► (클라이언트가 아직 revoke 안 함) → 현재 상태 유지
 *                         → (클라이언트가 ownedPartitions에서 제거 확인) → computeNextAssignment()
 *
 *  UNRELEASED_PARTITIONS → computeNextAssignment() (다른 멤버의 revoke 완료 시 할당 가능해짐)
 *
 *  UNKNOWN ──────────────► 멤버 펜싱 (FencedMemberEpochException), 처음부터 재조정
 *
 * </pre>
 *
 * <p>computeNextAssignment() 집합 연산:
 * <pre>
 *   assignedPartitions        = 현재할당 ∩ 목표할당
 *   partitionsPendingRevocation = 현재할당 - assignedPartitions  (= 현재할당 - 목표할당)
 *   partitionsPendingAssignment = 목표할당 - assignedPartitions - 다른멤버가_아직_보유중인파티션
 * </pre>
 */
public class CurrentAssignmentBuilder {
    /**
     * The consumer group member which is reconciled.
     */
    private final ConsumerGroupMember member;

    /**
     * The metadata image.
     */
    private CoordinatorMetadataImage metadataImage = CoordinatorMetadataImage.EMPTY;

    /**
     * The target assignment epoch.
     */
    private int targetAssignmentEpoch;

    /**
     * The target assignment.
     */
    private Assignment targetAssignment;

    /**
     * Whether the member has changed its subscription on the current heartbeat.
     */
    private boolean hasSubscriptionChanged;

    /**
     * The resolved regular expressions.
     */
    private Map<String, ResolvedRegularExpression> resolvedRegularExpressions = Map.of();

    /**
     * A function which returns the current epoch of a topic-partition or -1 if the
     * topic-partition is not assigned. The current epoch is the epoch of the current owner.
     */
    private BiFunction<Uuid, Integer, Integer> currentPartitionEpoch;

    /**
     * The partitions owned by the consumer. This is directly provided by the member in the
     * ConsumerGroupHeartbeat request.
     */
    private List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions;

    /**
     * Constructs the CurrentAssignmentBuilder based on the current state of the
     * provided consumer group member.
     *
     * @param member The consumer group member that must be reconciled.
     */
    public CurrentAssignmentBuilder(ConsumerGroupMember member) {
        this.member = Objects.requireNonNull(member);
    }

    /**
     * Sets the metadata image.
     *
     * @param metadataImage    The metadata image.
     * @return This object.
     */
    public CurrentAssignmentBuilder withMetadataImage(
        CoordinatorMetadataImage metadataImage
    ) {
        this.metadataImage = metadataImage;
        return this;
    }

    /**
     * Sets the target assignment epoch and the target assignment that the
     * consumer group member must be reconciled to.
     *
     * @param targetAssignmentEpoch The target assignment epoch.
     * @param targetAssignment      The target assignment.
     * @return This object.
     */
    public CurrentAssignmentBuilder withTargetAssignment(
        int targetAssignmentEpoch,
        Assignment targetAssignment
    ) {
        this.targetAssignmentEpoch = targetAssignmentEpoch;
        this.targetAssignment = Objects.requireNonNull(targetAssignment);
        return this;
    }

    /**
     * Sets whether the member has changed its subscription on the current heartbeat.
     *
     * @param hasSubscriptionChanged If true, always removes unsubscribed topics from the current assignment.
     * @return This object.
     */
    public CurrentAssignmentBuilder withHasSubscriptionChanged(
        boolean hasSubscriptionChanged
    ) {
        this.hasSubscriptionChanged = hasSubscriptionChanged;
        return this;
    }

    /**
     * Sets the resolved regular expressions.
     *
     * @param resolvedRegularExpressions The resolved regular expressions.
     * @return This object.
     */
    public CurrentAssignmentBuilder withResolvedRegularExpressions(
        Map<String, ResolvedRegularExpression> resolvedRegularExpressions
    ) {
        this.resolvedRegularExpressions = resolvedRegularExpressions;
        return this;
    }

    /**
     * Sets a BiFunction which allows to retrieve the current epoch of a
     * partition. This is used by the state machine to determine if a
     * partition is free or still used by another member.
     *
     * @param currentPartitionEpoch A BiFunction which gets the epoch of a
     *                              topic id / partitions id pair.
     * @return This object.
     */
    public CurrentAssignmentBuilder withCurrentPartitionEpoch(
        BiFunction<Uuid, Integer, Integer> currentPartitionEpoch
    ) {
        this.currentPartitionEpoch = Objects.requireNonNull(currentPartitionEpoch);
        return this;
    }

    /**
     * Sets the partitions currently owned by the member. This comes directly
     * from the last ConsumerGroupHeartbeat request. This is used to determine
     * if the member has revoked the necessary partitions.
     *
     * @param ownedTopicPartitions A list of topic-partitions.
     * @return This object.
     */
    public CurrentAssignmentBuilder withOwnedTopicPartitions(
        List<ConsumerGroupHeartbeatRequestData.TopicPartitions> ownedTopicPartitions
    ) {
        this.ownedTopicPartitions = ownedTopicPartitions;
        return this;
    }

    /**
     * Builds the next state for the member or keep the current one if it
     * is not possible to move forward with the current state.
     *
     * <p>[MODERN-1] ConsumerGroupHeartbeat 수신 시마다 호출된다.
     * 현재 멤버 상태(MemberState)에 따라 다음 행동을 결정하며,
     * 불변인 ConsumerGroupMember 객체를 새로 생성하여 반환한다(기존 객체는 수정되지 않음).
     *
     * <p>Classic 프로토콜과의 핵심 차이:
     * - 그룹 전체가 멈추는(stop-the-world) 대신 멤버별로 독립적으로 수렴한다.
     * - revoke는 클라이언트가 ownedTopicPartitions에서 파티션을 제거함으로써 확인된다.
     * - assign은 서버가 HeartbeatResponse의 assignment에 새 파티션을 포함시킴으로써 전달된다.
     *
     * @return A new ConsumerGroupMember or the current one.
     */
    public ConsumerGroupMember build() {
        switch (member.state()) {
            case STABLE:
                // When the member is in the STABLE state, we verify if a newer
                // epoch (or target assignment) is available. If it is, we can
                // reconcile the member towards it. Otherwise, we ensure the
                // assignment is consistent with the subscribed topics, if changed.
                // [MODERN-2a] STABLE 상태: 멤버 에포크가 목표 에포크보다 낮으면 새 할당이 존재하므로 조정을 시작한다.
                // 구독 토픽이 변경된 경우: 더 이상 구독하지 않는 토픽의 파티션을 revoke한다.
                if (member.memberEpoch() != targetAssignmentEpoch) {
                    return computeNextAssignment(
                        member.memberEpoch(),
                        member.assignedPartitions()
                    );
                } else if (hasSubscriptionChanged) {
                    return updateCurrentAssignment(
                        member.memberEpoch(),
                        member.assignedPartitions()
                    );
                } else {
                    return member;
                }

            case UNREVOKED_PARTITIONS:
                // When the member is in the UNREVOKED_PARTITIONS state, we wait
                // until the member has revoked the necessary partitions. They are
                // considered revoked when they are not anymore reported in the
                // owned partitions set in the ConsumerGroupHeartbeat API.
                // Additional partitions may need revoking when the member's
                // subscription changes.

                // If the member provides its owned partitions. We verify if it still
                // owns any of the revoked partitions. If it does, we cannot progress.
                // [MODERN-2b] UNREVOKED_PARTITIONS 상태: ownedTopicPartitions에 아직 revoke 대상 파티션이 있으면 대기한다.
                // 클라이언트가 해당 파티션을 own 목록에서 제거한 다음 하트비트를 보내야 진행할 수 있다.
                // revoke 완료 확인 후 computeNextAssignment()로 다음 단계로 진행한다.
                if (ownsRevokedPartitions(member.partitionsPendingRevocation())) {
                    if (hasSubscriptionChanged) {
                        return updateCurrentAssignment(
                            member.memberEpoch(),
                            member.assignedPartitions()
                        );
                    } else {
                        return member;
                    }
                }

                // When the member has revoked all the pending partitions, we can
                // reconcile its state towards the latest target assignment.
                return computeNextAssignment(
                    member.memberEpoch(),
                    member.assignedPartitions()
                );

            case UNRELEASED_PARTITIONS:
                // When the member is in the UNRELEASED_PARTITIONS, we reconcile the
                // member towards the latest target assignment. This will assign any
                // of the unreleased partitions when they become available.
                // [MODERN-2c] UNRELEASED_PARTITIONS 상태: 다른 멤버가 아직 보유 중이던 파티션이 해제됐을 수 있으므로
                // 매 하트비트마다 computeNextAssignment를 재시도하여 할당 가능 여부를 확인한다.
                return computeNextAssignment(
                    member.memberEpoch(),
                    member.assignedPartitions()
                );

            case UNKNOWN:
                // We could only end up in this state if a new state is added in the
                // future and the group coordinator is downgraded. In this case, the
                // best option is to fence the member to force it to rejoin the group
                // without any partitions and to reconcile it again from scratch.
                if (ownedTopicPartitions == null || !ownedTopicPartitions.isEmpty()) {
                    throw new FencedMemberEpochException("The consumer group member is in a unknown state. "
                        + "The member must abandon all its partitions and rejoin.");
                }

                return computeNextAssignment(
                    targetAssignmentEpoch,
                    member.assignedPartitions()
                );
        }

        return member;
    }

    /**
     * Decides whether the current ownedTopicPartitions contains any partition that is pending revocation.
     *
     * @param assignment The assignment that has the partitions pending revocation.
     * @return A boolean based on the condition mentioned above.
     */
    private boolean ownsRevokedPartitions(
        Map<Uuid, Set<Integer>> assignment
    ) {
        if (ownedTopicPartitions == null) return true;

        for (ConsumerGroupHeartbeatRequestData.TopicPartitions topicPartitions : ownedTopicPartitions) {
            Set<Integer> partitionsPendingRevocation =
                assignment.getOrDefault(topicPartitions.topicId(), Set.of());

            for (Integer partitionId : topicPartitions.partitions()) {
                if (partitionsPendingRevocation.contains(partitionId)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Updates the current assignment, removing any partitions that are not part of the subscribed topics.
     * This method is a lot faster than running the full reconciliation logic in computeNextAssignment.
     *
     * @param memberEpoch               The epoch of the member to use.
     * @param memberAssignedPartitions  The assigned partitions of the member to use.
     * @return A new ConsumerGroupMember.
     */
    private ConsumerGroupMember updateCurrentAssignment(
        int memberEpoch,
        Map<Uuid, Set<Integer>> memberAssignedPartitions
    ) {
        Set<Uuid> subscribedTopicIds = subscribedTopicIds();

        // Reuse the original map if no topics need to be removed.
        Map<Uuid, Set<Integer>> newAssignedPartitions;
        Map<Uuid, Set<Integer>> newPartitionsPendingRevocation;
        if (subscribedTopicIds.isEmpty() && member.partitionsPendingRevocation().isEmpty()) {
            newAssignedPartitions = Map.of();
            newPartitionsPendingRevocation = memberAssignedPartitions;
        } else {
            newAssignedPartitions = memberAssignedPartitions;
            newPartitionsPendingRevocation = new HashMap<>(member.partitionsPendingRevocation());
            for (Map.Entry<Uuid, Set<Integer>> entry : memberAssignedPartitions.entrySet()) {
                if (!subscribedTopicIds.contains(entry.getKey())) {
                    if (newAssignedPartitions == memberAssignedPartitions) {
                        newAssignedPartitions = new HashMap<>(memberAssignedPartitions);
                        newPartitionsPendingRevocation = new HashMap<>(member.partitionsPendingRevocation());
                    }
                    newAssignedPartitions.remove(entry.getKey());
                    newPartitionsPendingRevocation.merge(
                        entry.getKey(),
                        entry.getValue(),
                        (existing, additional) -> {
                            existing = new HashSet<>(existing);
                            existing.addAll(additional);
                            return existing;
                        }
                    );
                }
            }
        }

        if (newAssignedPartitions == memberAssignedPartitions) {
            // If no partitions were removed, we can return the member as is.
            return member;
        }

        if (!newPartitionsPendingRevocation.isEmpty() && ownsRevokedPartitions(newPartitionsPendingRevocation)) {
            return new ConsumerGroupMember.Builder(member)
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .updateMemberEpoch(memberEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(newPartitionsPendingRevocation)
                .build();
        } else {
            // There were partitions removed, but they were already revoked.
            // Keep the member in the current state and shrink the assigned partitions.

            // We do not expect to be in the UNREVOKED_PARTITIONS state here. The full
            // reconciliation logic should handle the case where the member has revoked all its
            // partitions pending revocation.
            return new ConsumerGroupMember.Builder(member)
                .updateMemberEpoch(memberEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .build();
        }
    }

    /**
     * Computes the next assignment.
     *
     * <p>[MODERN-3] 목표 할당(targetAssignment)을 향해 한 단계씩 수렴하는 핵심 집합 연산:
     * <ol>
     *   <li>각 토픽에 대해 세 집합을 계산한다:
     *     <ul>
     *       <li>assignedPartitions = 현재할당 ∩ 목표할당 (즉시 유지 가능)</li>
     *       <li>partitionsPendingRevocation = 현재할당 - 목표할당 (클라이언트에게 revoke 요청)</li>
     *       <li>partitionsPendingAssignment = 목표할당 - 현재할당 - 다른멤버_보유중 (즉시 할당 가능)</li>
     *     </ul>
     *   </li>
     *   <li>currentPartitionEpoch 함수로 파티션이 다른 멤버에 의해 아직 보유됐는지 확인한다.
     *       epoch != -1이면 아직 해제되지 않은 것이므로 UNRELEASED 처리.</li>
     *   <li>결과에 따라 다음 상태를 결정한다:
     *     <ul>
     *       <li>revoke 필요 → UNREVOKED_PARTITIONS (에포크 유지, 클라이언트가 revoke 확인할 때까지 대기)</li>
     *       <li>assign 가능 (+ 미해제 파티션 있음) → UNRELEASED_PARTITIONS (에포크 전진, 해제 대기)</li>
     *       <li>assign 가능 (미해제 없음) → STABLE (에포크 전진, 조정 완료)</li>
     *       <li>assign할 것 없고 미해제만 있음 → UNRELEASED_PARTITIONS</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * @param memberEpoch               The epoch of the member to use. This may be different
     *                                  from the epoch in {@link CurrentAssignmentBuilder#member}.
     * @param memberAssignedPartitions  The assigned partitions of the member to use.
     * @return A new ConsumerGroupMember.
     */
    private ConsumerGroupMember computeNextAssignment(
        int memberEpoch,
        Map<Uuid, Set<Integer>> memberAssignedPartitions
    ) {
        Set<Uuid> subscribedTopicIds = subscribedTopicIds();

        boolean hasUnreleasedPartitions = false;
        Map<Uuid, Set<Integer>> newAssignedPartitions = new HashMap<>();
        Map<Uuid, Set<Integer>> newPartitionsPendingRevocation = new HashMap<>();
        Map<Uuid, Set<Integer>> newPartitionsPendingAssignment = new HashMap<>();

        Set<Uuid> allTopicIds = new HashSet<>(targetAssignment.partitions().keySet());
        allTopicIds.addAll(memberAssignedPartitions.keySet());

        for (Uuid topicId : allTopicIds) {
            Set<Integer> target = targetAssignment.partitions()
                .getOrDefault(topicId, Set.of());
            Set<Integer> currentAssignedPartitions = memberAssignedPartitions
                .getOrDefault(topicId, Set.of());

            // If the member is no longer subscribed to the topic, treat its target assignment as empty.
            if (!subscribedTopicIds.contains(topicId)) {
                target = Set.of();
            }

            // New Assigned Partitions = Previous Assigned Partitions ∩ Target
            Set<Integer> assignedPartitions = new HashSet<>(currentAssignedPartitions);
            assignedPartitions.retainAll(target);

            // Partitions Pending Revocation = Previous Assigned Partitions - New Assigned Partitions
            Set<Integer> partitionsPendingRevocation = new HashSet<>(currentAssignedPartitions);
            partitionsPendingRevocation.removeAll(assignedPartitions);

            // Partitions Pending Assignment = Target - New Assigned Partitions - Unreleased Partitions
            Set<Integer> partitionsPendingAssignment = new HashSet<>(target);
            partitionsPendingAssignment.removeAll(assignedPartitions);
            hasUnreleasedPartitions = partitionsPendingAssignment.removeIf(partitionId ->
                currentPartitionEpoch.apply(topicId, partitionId) != -1 &&
                // Don't consider a partition unreleased if it is owned by the current member
                // because it is pending revocation. This is safe to do since only a single member
                // can own a partition at a time.
                !member.partitionsPendingRevocation().getOrDefault(topicId, Set.of()).contains(partitionId)
            ) || hasUnreleasedPartitions;

            if (!assignedPartitions.isEmpty()) {
                newAssignedPartitions.put(topicId, assignedPartitions);
            }

            if (!partitionsPendingRevocation.isEmpty()) {
                newPartitionsPendingRevocation.put(topicId, partitionsPendingRevocation);
            }

            if (!partitionsPendingAssignment.isEmpty()) {
                newPartitionsPendingAssignment.put(topicId, partitionsPendingAssignment);
            }
        }

        if (!newPartitionsPendingRevocation.isEmpty() && ownsRevokedPartitions(newPartitionsPendingRevocation)) {
            // If there are partitions to be revoked, the member remains in its current
            // epoch and requests the revocation of those partitions. It transitions to
            // the UNREVOKED_PARTITIONS state to wait until the client acknowledges the
            // revocation of the partitions.
            return new ConsumerGroupMember.Builder(member)
                .setState(MemberState.UNREVOKED_PARTITIONS)
                .updateMemberEpoch(memberEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(newPartitionsPendingRevocation)
                .build();
        } else if (!newPartitionsPendingAssignment.isEmpty()) {
            // If there are partitions to be assigned, the member transitions to the
            // target epoch and requests the assignment of those partitions. Note that
            // the partitions are directly added to the assigned partitions set. The
            // member transitions to the STABLE state or to the UNRELEASED_PARTITIONS
            // state depending on whether there are unreleased partitions or not.
            newPartitionsPendingAssignment.forEach((topicId, partitions) -> newAssignedPartitions
                .computeIfAbsent(topicId, __ -> new HashSet<>())
                .addAll(partitions));
            MemberState newState = hasUnreleasedPartitions ? MemberState.UNRELEASED_PARTITIONS : MemberState.STABLE;
            return new ConsumerGroupMember.Builder(member)
                .setState(newState)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Map.of())
                .build();
        } else if (hasUnreleasedPartitions) {
            // If there are no partitions to be revoked nor to be assigned but some
            // partitions are not available yet, the member transitions to the target
            // epoch, to the UNRELEASED_PARTITIONS state and waits.
            return new ConsumerGroupMember.Builder(member)
                .setState(MemberState.UNRELEASED_PARTITIONS)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Map.of())
                .build();
        } else {
            // Otherwise, the member transitions to the target epoch and to the
            // STABLE state.
            return new ConsumerGroupMember.Builder(member)
                .setState(MemberState.STABLE)
                .updateMemberEpoch(targetAssignmentEpoch)
                .setAssignedPartitions(newAssignedPartitions)
                .setPartitionsPendingRevocation(Map.of())
                .build();
        }
    }

    /**
     * Gets the set of topic IDs that the member is subscribed to.
     *
     * @return The set of topic IDs that the member is subscribed to.
     */
    private Set<Uuid> subscribedTopicIds() {
        Set<String> subscriptions = member.subscribedTopicNames();
        String subscribedTopicRegex = member.subscribedTopicRegex();
        if (subscribedTopicRegex != null && !subscribedTopicRegex.isEmpty()) {
            ResolvedRegularExpression resolvedRegularExpression = resolvedRegularExpressions.get(subscribedTopicRegex);
            if (resolvedRegularExpression != null) {
                if (subscriptions.isEmpty()) {
                    subscriptions = resolvedRegularExpression.topics();
                } else if (!resolvedRegularExpression.topics().isEmpty()) {
                    subscriptions = new UnionSet<>(subscriptions, resolvedRegularExpression.topics());
                }
            } else {
                // Treat an unresolved regex as matching no topics, to be conservative.
            }
        }

        return new TopicIds(subscriptions, metadataImage);
    }
}
