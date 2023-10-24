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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.MemberIdRequiredException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, List)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 */
public abstract class AbstractCoordinator implements Closeable {
    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";
    public static final int JOIN_GROUP_TIMEOUT_LAPSE = 5000;

    // 消费端成员状态
    protected enum MemberState {
        UNJOINED,             // the client is not part of a group
        PREPARING_REBALANCE,  // the client has sent the join group request, but have not received response
        COMPLETING_REBALANCE, // the client has received join group response, but have not received assignment
        STABLE;               // the client has joined and is sending heartbeats

        public boolean hasNotJoinedGroup() {
            return equals(UNJOINED) || equals(PREPARING_REBALANCE);
        }
    }

    private final Logger log;
    // 心跳对象
    private final Heartbeat heartbeat;
    // 消费者协调器的指标信息
    private final GroupCoordinatorMetrics sensors;
    // 消费者组重分配配置
    private final GroupRebalanceConfig rebalanceConfig;

    protected final Time time;
    // 负责网络通信
    protected final ConsumerNetworkClient client;

    // Node类型，保存着服务端 GroupCoordinator 的节点。
    private Node coordinator = null;
    // 标记重新发送 JoinGroupRequest 请求。
    private boolean rejoinNeeded = true;
    // 是否重新发送 JoinGroupRequest 请求。
    private boolean needsJoinPrepare = true;
    // 维持心跳的线程
    private HeartbeatThread heartbeatThread = null;
    // 加入 GroupCoordinator 的异步请求类对象
    private RequestFuture<ByteBuffer> joinFuture = null;
    // 客户端查找 GroupCoordinator 的异步请求类对象。
    private RequestFuture<Void> findCoordinatorFuture = null;
    // 查找协调器失败异常
    volatile private RuntimeException findCoordinatorException = null;
    // 服务端 GroupCoordinator 的年代信息
    private Generation generation = Generation.NO_GENERATION;
    // 最后一次 rebalance 的开始时间
    private long lastRebalanceStartMs = -1L;
    // 最后一次 rebalance 的结束时间
    private long lastRebalanceEndMs = -1L;

    // 初始成员状态
    protected MemberState state = MemberState.UNJOINED;


    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time) {
        Objects.requireNonNull(rebalanceConfig.groupId,
                               "Expected a non-null group id for coordinator construction");
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(AbstractCoordinator.class);
        this.client = client;
        this.time = time;
        this.heartbeat = new Heartbeat(rebalanceConfig, time);
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param protocol The protocol selected by the coordinator
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group. If this call fails with an exception,
     * then it will be retried using the same assignment state on the next call to {@link #ensureActiveGroup()}.
     *
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Invoked prior to each leave group event. This is typically used to cleanup assigned partitions;
     * note it is triggered by the consumer's API caller thread (i.e. background heartbeat thread would
     * not trigger it even if it tries to force leaving group upon heartbeat session expiration)
     */
    protected void onLeavePrepare() {}

    /**
     * Visible for testing.
     *
     * Ensure that the coordinator is ready to receive requests.
     *
     * @param timer Timer bounding how long this method can block
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     */
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        // 1、如果还未获取到 Coordinator 需要先与其进行通信
        if (!coordinatorUnknown())
            return true;

        do {
            if (findCoordinatorException != null && !(findCoordinatorException instanceof RetriableException)) {
                final RuntimeException fatalException = findCoordinatorException;
                findCoordinatorException = null;
                throw fatalException;
            }
            // 2、调用 AbstractCoordinator#lookupCoordinator() 方法生成异步请求，并将其存入到请求队列中，
            //    即将请求存入 ConsumerNetworkClient#unsent 队列中，此处设置请求类型 KafkaApis.FIND_COORDINATOR
            final RequestFuture<Void> future = lookupCoordinator();
            // 3、调用 ConsumerNetworkClient#poll() 实际发起网络请求，并监听服务端的响应，
            //    如异步请求完成则退出循环。此时获取到每一个请求都绑定 SelectionKey.OP_WRITE 事件
            client.poll(future, timer);

            // 4、如果还没回调完成则说明是超时的
            if (!future.isDone()) {
                // ran out of time
                break;
            }

            // 4、异常处理
            if (future.failed()) {
                if (future.isRetriable()) {
                    log.debug("Coordinator discovery failed, refreshing metadata", future.exception());
                    client.awaitMetadataUpdate(timer);
                } else
                    throw future.exception();
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                markCoordinatorUnknown("coordinator unavailable");
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }

            // 5、不断尝试获取 GroupCoordinator 直到与 GroupCoordinator 通信成功或超时时跳出循环。
        } while (coordinatorUnknown() && timer.notExpired());

        return !coordinatorUnknown();
    }

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            // 首先调用 ConsumerNetworkClient#leastLoadedNode()
            // 找一个最小负载节点作为询问协调者
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                log.debug("No broker available to send FindCoordinator request");
                return RequestFuture.noBrokersAvailable();
            } else {
                // 发送 FIND_COORDINATOR 请求，其实是将该请求放入未发送队列中 unsent
                findCoordinatorFuture = sendFindCoordinatorRequest(node);
                // remember the exception even after the future is cleared so that
                // it can still be thrown by the ensureCoordinatorReady caller

                // 调用 AbstractCoordinator#sendFindCoordinatorRequest() 方法生成发送给目标节点的异步请求，
                // 发送 FIND_COORDINATOR 请求，其实是将该请求放入未发送队列中 unsent
                findCoordinatorFuture.addListener(new RequestFutureListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {} // do nothing

                    @Override
                    public void onFailure(RuntimeException e) {
                        findCoordinatorException = e;
                    }
                });
            }
        }
        return findCoordinatorFuture;
    }

    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes) or whether a
     * rejoin request is already in flight and needs to be completed.
     *
     * @return true if it should, false otherwise
     */
    protected synchronized boolean rejoinNeededOrPending() {
        // if there's a pending joinFuture, we should try to complete handling it.
        return rejoinNeeded || joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     *
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.

                // 如果心跳线程发生故障，将其设置为 null 并抛出异常。如果用户捕获了异常，
                // 则下一次调用 ensureActiveGroup() 将会创建一个新的 heartbeat 线程。
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }
            // Awake the heartbeat thread if needed
            // 如果需要唤醒心跳线程，则调用 notify() 方法唤醒它
            if (heartbeat.shouldHeartbeat(now)) {
                notify();
            }

            // 执行心跳操作
            heartbeat.poll(now);
        }
    }

    protected synchronized long timeToNextHeartbeat(long now) {
        // if we have not joined the group or we are preparing rebalance,
        // we don't need to send heartbeats
        if (state.hasNotJoinedGroup())
            return Long.MAX_VALUE;
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    public void ensureActiveGroup() {
        while (!ensureActiveGroup(time.timer(Long.MAX_VALUE))) {
            log.warn("still waiting to ensure active group");
        }
    }

    /**
     * Ensure the group is active (i.e., joined and synced)
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the group is active
     */
    boolean ensureActiveGroup(final Timer timer) {
        // always ensure that the coordinator is ready because we may have been disconnected
        // when sending heartbeats and does not necessarily require us to rejoin the group.

        // 1、发送 FIND_COORDINATOR 请求给 Broker，获取该消费者组对应的 Broker 服务节点
        if (!ensureCoordinatorReady(timer)) {
            return false;
        }

        // 2、调用 AbstractCoordinator#startHeartbeatThreadIfNeeded() 方法尝试启动心跳线程，
        //    与消费者组协调器建立心跳连接，关于心跳线程的逻辑在下面详细剖析
        startHeartbeatThreadIfNeeded();

        // 3、调用 AbstractCoordinator#joinGroupIfNeeded() 方法尝试进入消费者加入消费者组的流程
        return joinGroupIfNeeded(timer);
    }

    private synchronized void startHeartbeatThreadIfNeeded() {
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }
    }

    private void closeHeartbeatThread() {
        HeartbeatThread thread;
        synchronized (this) {
            if (heartbeatThread == null)
                return;
            heartbeatThread.close();
            thread = heartbeatThread;
            heartbeatThread = null;
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for consumer heartbeat thread to close");
            throw new InterruptException(e);
        }
    }

    /**
     * Joins the group without starting the heartbeat thread.
     *
     * If this function returns true, the state must always be in STABLE and heartbeat enabled.
     * If this function returns false, the state can be in one of the following:
     *  * UNJOINED: got error response but times out before being able to re-join, heartbeat disabled
     *  * PREPARING_REBALANCE: not yet received join-group response before timeout, heartbeat disabled
     *  * COMPLETING_REBALANCE: not yet received sync-group response before timeout, hearbeat enabled
     *
     * Visible for testing.
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the operation succeeded
     */
    boolean joinGroupIfNeeded(final Timer timer) {
        // 如果需要重新加入或正在等待重新加入，则继续循环
        while (rejoinNeededOrPending()) {
            if (!ensureCoordinatorReady(timer)) {
                // 确保协调器就绪，如果协调器不可用，则返回 false
                return false;
            }

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a second
            // time if the client is woken up before a pending rebalance completes. This must be called
            // on each iteration of the loop because an event requiring a rebalance (such as a metadata
            // refresh which changes the matched subscription set) can occur while another rebalance is
            // still in progress.
            if (needsJoinPrepare) {
                // need to set the flag before calling onJoinPrepare since the user callback may throw
                // exception, in which case upon retry we should not retry onJoinPrepare either.
                // 如果需要准备加入群组，则调用 onJoinPrepare 方法
                needsJoinPrepare = false;
                onJoinPrepare(generation.generationId, generation.memberId);
            }

            // 1、准备请求，准备 JOIN_GROUP 类型请求，存储到 unsent 队列中，
            // 调用 AbstractCoordinator#initiateJoinGroup() 方法生成加入消费者组的 JoinGroup 异步请求，并通过轮询等待响应
            final RequestFuture<ByteBuffer> future = initiateJoinGroup();

            // 2、调用 ConsumerNetworkClinet#poll() 方法发送请求，轮询处理待发送队列中的所有可发送的请求，这部分在上一节中有分析，不再赘述
            client.poll(future, timer);
            if (!future.isDone()) {
                // we ran out of time
                // 如果等待超时，则返回 false
                return false;
            }

            if (future.succeeded()) {
                // 如果 JoinGroup 请求成功
                Generation generationSnapshot;
                MemberState stateSnapshot;

                // Generation data maybe concurrently cleared by Heartbeat thread.
                // Can't use synchronized for {@code onJoinComplete}, because it can be long enough
                // and shouldn't block heartbeat thread.
                // See {@link PlaintextConsumerTest#testMaxPollIntervalMsDelayInAssignment}
                synchronized (AbstractCoordinator.this) {
                    // 复制当前的 generation 和 state 数据，以防止在回调期间被并发修改
                    generationSnapshot = this.generation;
                    stateSnapshot = this.state;
                }

                if (!generationSnapshot.equals(Generation.NO_GENERATION) && stateSnapshot == MemberState.STABLE) {
                    // Duplicate the buffer in case `onJoinComplete` does not complete and needs to be retried.
                    // 如果当前的 generation 和 state 数据有效
                    ByteBuffer memberAssignment = future.value().duplicate();

                    // 3、如果加入消费者组的异步请求成功完成，则协调器会将当前消费者负责的分区下发过来，
                    // 消费者调用 ConsumerCoordinator#onJoinComplete() 执行加入群组后的逻辑
                    onJoinComplete(generationSnapshot.generationId, generationSnapshot.memberId, generationSnapshot.protocolName, memberAssignment);

                    // Generally speaking we should always resetJoinGroupFuture once the future is done, but here
                    // we can only reset the join group future after the completion callback returns. This ensures
                    // that if the callback is woken up, we will retry it on the next joinGroupIfNeeded.
                    // And because of that we should explicitly trigger resetJoinGroupFuture in other conditions below.
                    resetJoinGroupFuture();
                    needsJoinPrepare = true;
                } else {
                    log.info("Generation data was cleared by heartbeat thread to {} and state is now {} before " +
                         "the rebalance callback is triggered, marking this rebalance as failed and retry",
                         generationSnapshot, stateSnapshot);
                    resetStateAndRejoin();
                    resetJoinGroupFuture();
                }
            } else {
                // 如果 JoinGroup 请求失败
                final RuntimeException exception = future.exception();

                // we do not need to log error for memberId required,
                // since it is not really an error and is transient
                if (!(exception instanceof MemberIdRequiredException)) {
                    log.info("Rebalance failed.", exception);
                }

                resetJoinGroupFuture();
                if (exception instanceof UnknownMemberIdException ||
                    exception instanceof RebalanceInProgressException ||
                    exception instanceof IllegalGenerationException ||
                    exception instanceof MemberIdRequiredException)
                    continue;
                else if (!future.isRetriable())
                    throw exception;

                resetStateAndRejoin();
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }
        }
        return true;
    }

    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt
        // to rejoin before the pending rebalance has completed.

        // 1、为了避免在调用 poll 方法后被用户唤醒而错误地尝试重新加入消费者组，将 joinFuture 存储起来。
        if (joinFuture == null) {
            state = MemberState.PREPARING_REBALANCE;
            // a rebalance can be triggered consecutively if the previous one failed,
            // in this case we would not update the start time.

            // 2、如果之前的重平衡失败，可能会连续触发重平衡，这种情况下不更新开始时间。
            if (lastRebalanceStartMs == -1L)
                lastRebalanceStartMs = time.milliseconds();
            joinFuture = sendJoinGroupRequest();
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // joinFuture 成功完成时不做任何操作，所有的处理逻辑已经在 SyncGroupResponseHandler 中了
                    // do nothing since all the handler logic are in SyncGroupResponseHandler already
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin;
                    // this can be triggered when either join or sync request failed

                    // 请求完成后处理失败的情况
                    // 如果在唤醒后完成了加入消费者组，则忽略异常，重新加入群组
                    synchronized (AbstractCoordinator.this) {
                        sensors.failedRebalanceSensor.record();
                    }
                }
            });
        }
        return joinFuture;
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, List)} if
     * elected leader by the coordinator.
     *
     * NOTE: This is visible only for testing
     *
     * @return A request future which wraps the assignment returned from the group leader
     */
    RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        // 如果 coordinator 不可用，则返回 RequestFuture，其状态为未知协调器
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        // 向协调器发送 JoinGroup 请求
        log.info("(Re-)joining group");
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                new JoinGroupRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setSessionTimeoutMs(this.rebalanceConfig.sessionTimeoutMs)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setProtocolType(protocolType())
                        .setProtocols(metadata())
                        .setRebalanceTimeoutMs(this.rebalanceConfig.rebalanceTimeoutMs)
        );

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);

        // Note that we override the request timeout using the rebalance timeout since that is the
        // maximum time that it may block on the coordinator. We add an extra 5 seconds for small delays.
        // 由于重平衡超时时间是协调器可能会阻塞的最长时间，因此我们使用重平衡超时时间来覆盖请求超时时间。
        // 我们额外加了 5 秒用于解决可能出现的小延迟。
        int joinGroupTimeoutMs = Math.max(client.defaultRequestTimeoutMs(),
            rebalanceConfig.rebalanceTimeoutMs + JOIN_GROUP_TIMEOUT_LAPSE);
        return client.send(coordinator, requestBuilder, joinGroupTimeoutMs)
                .compose(new JoinGroupResponseHandler(generation));
    }

    //  请求 JOIN_GROUP 的响应处理器
    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        private JoinGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = joinResponse.error();
            if (error == Errors.NONE) {
                if (isProtocolTypeInconsistent(joinResponse.data().protocolType())) {
                    log.error("JoinGroup failed: Inconsistent Protocol Type, received {} but expected {}",
                        joinResponse.data().protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    // 成功分支，处理加入组成功的情况
                    log.debug("Received successful JoinGroup response: {}", joinResponse);
                    sensors.joinSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        // 修改状态，完成 再平衡
                        if (state != MemberState.PREPARING_REBALANCE) {
                            // if the consumer was woken up before a rebalance completes, we may have already left
                            // the group. In this case, we do not want to continue with the sync group.
                            future.raise(new UnjoinedGroupException());
                        } else {
                            state = MemberState.COMPLETING_REBALANCE;

                            // we only need to enable heartbeat thread whenever we transit to
                            // COMPLETING_REBALANCE state since we always transit from this state to STABLE
                            if (heartbeatThread != null)
                                heartbeatThread.enable();

                            AbstractCoordinator.this.generation = new Generation(
                                joinResponse.data().generationId(),
                                joinResponse.data().memberId(), joinResponse.data().protocolName());

                            log.info("Successfully joined group with generation {}", AbstractCoordinator.this.generation);

                            // 判断自己是不是 leader
                            if (joinResponse.isLeader()) {
                                // 根据协调器返回的消费者组 leader 的 id 判断自身是否是消费者组 leader，
                                // 如是的话则调用 AbstractCoordinator#onJoinLeader() 进行指定分区分配，并再次发送 SYNC_GROUP 请求
                                onJoinLeader(joinResponse).chain(future);
                            } else {
                                onJoinFollower().chain(future);
                            }
                        }
                    }
                }
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                // 处理协调器正在加载组的情况
                log.info("JoinGroup failed: Coordinator {} is loading the group.", coordinator());
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // 处理未知成员ID的情况
                log.info("JoinGroup failed: {} Need to re-join the group. Sent generation was {}",
                         error.message(), sentGeneration);
                // only need to reset the member id if generation has not been changed,
                // then retry immediately
                if (generationUnchanged())
                    resetGenerationOnResponseError(ApiKeys.JOIN_GROUP, error);

                future.raise(error);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                // 处理协调器不可用或者不是协调器的情况
                // re-discover the coordinator and retry with backoff
                markCoordinatorUnknown(error);
                log.info("JoinGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                          error.message(), sentGeneration);
                future.raise(error);
            } else if (error == Errors.FENCED_INSTANCE_ID) {
                // 处理组的实例ID被另一个实例锁定的情况
                // for join-group request, even if the generation has changed we would not expect the instance id
                // gets fenced, and hence we always treat this as a fatal error
                log.error("JoinGroup failed: The group instance id {} has been fenced by another instance. " +
                              "Sent generation was {}", rebalanceConfig.groupInstanceId, sentGeneration);
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID
                    || error == Errors.GROUP_AUTHORIZATION_FAILED
                    || error == Errors.GROUP_MAX_SIZE_REACHED) {
                // 处理组协议不一致、会话超时无效、组ID无效、组授权失败、组达到最大大小的情况
                // log the error and re-throw the exception
                log.error("JoinGroup failed due to fatal error: {}", error.message());
                if (error == Errors.GROUP_MAX_SIZE_REACHED) {
                    future.raise(new GroupMaxSizeReachedException("Consumer group " + rebalanceConfig.groupId +
                            " already has the configured maximum number of members."));
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(error);
                }
            } else if (error == Errors.UNSUPPORTED_VERSION) {
                // 处理不支持的版本错误的情况
                log.error("JoinGroup failed due to unsupported version error. Please unset field group.instance.id " +
                          "and retry to see if the problem resolves");
                future.raise(error);
            } else if (error == Errors.MEMBER_ID_REQUIRED) {
                // 处理需要成员ID的情况
                // Broker requires a concrete member id to be allowed to join the group. Update member id
                // and send another join group request in next cycle.
                String memberId = joinResponse.data().memberId();
                log.debug("JoinGroup failed due to non-fatal error: {} Will set the member id as {} and then rejoin. " +
                              "Sent generation was  {}", error, memberId, sentGeneration);
                synchronized (AbstractCoordinator.this) {
                    AbstractCoordinator.this.generation = new Generation(OffsetCommitRequest.DEFAULT_GENERATION_ID, memberId, null);
                }
                future.raise(error);
            } else {
                // unexpected error, throw the exception
                log.error("JoinGroup failed due to unexpected error: {}", error.message());
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest.Builder requestBuilder =
                new SyncGroupRequest.Builder(
                        new SyncGroupRequestData()
                                .setGroupId(rebalanceConfig.groupId)
                                .setMemberId(generation.memberId)
                                .setProtocolType(protocolType())
                                .setProtocolName(generation.protocolName)
                                .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                                .setGenerationId(generation.generationId)
                                .setAssignments(Collections.emptyList())
                );
        log.debug("Sending follower SyncGroup to coordinator {} at generation {}: {}", this.coordinator, this.generation, requestBuilder);
        return sendSyncGroupRequest(requestBuilder);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            // 1、调用 ConsumerCoordinator#performAssignment() 进行分区分配
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.data().leader(), joinResponse.data().protocolName(),
                    joinResponse.data().members());

            // 生成SyncGroup 异步请求，调用 AbstractCoordinator#sendSyncGroupRequest() 方法将分配方案同步给协调器
            List<SyncGroupRequestData.SyncGroupRequestAssignment> groupAssignmentList = new ArrayList<>();
            for (Map.Entry<String, ByteBuffer> assignment : groupAssignment.entrySet()) {
                groupAssignmentList.add(new SyncGroupRequestData.SyncGroupRequestAssignment()
                        .setMemberId(assignment.getKey())
                        .setAssignment(Utils.toArray(assignment.getValue()))
                );
            }

            // ApiKeys.SYNC_GROUP 类型请求，给服务端发送制定完成的分区分配策略
            SyncGroupRequest.Builder requestBuilder =
                    new SyncGroupRequest.Builder(
                            new SyncGroupRequestData()
                                    .setGroupId(rebalanceConfig.groupId)
                                    .setMemberId(generation.memberId)
                                    .setProtocolType(protocolType())
                                    .setProtocolName(generation.protocolName)
                                    .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                                    .setGenerationId(generation.generationId)
                                    .setAssignments(groupAssignmentList)
                    );
            log.debug("Sending leader SyncGroup to coordinator {} at generation {}: {}", this.coordinator, this.generation, requestBuilder);
            // 准备请求 存储到 unsent 队列
            return sendSyncGroupRequest(requestBuilder);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, requestBuilder)
                .compose(new SyncGroupResponseHandler(generation));
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        private SyncGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            Errors error = syncResponse.error();
            if (error == Errors.NONE) {
                if (isProtocolTypeInconsistent(syncResponse.data.protocolType())) {
                    log.error("SyncGroup failed due to inconsistent Protocol Type, received {} but expected {}",
                        syncResponse.data.protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    log.debug("Received successful SyncGroup response: {}", syncResponse);
                    sensors.syncSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        if (!generation.equals(Generation.NO_GENERATION) && state == MemberState.COMPLETING_REBALANCE) {
                            // check protocol name only if the generation is not reset
                            final String protocolName = syncResponse.data.protocolName();
                            final boolean protocolNameInconsistent = protocolName != null &&
                                !protocolName.equals(generation.protocolName);

                            if (protocolNameInconsistent) {
                                log.error("SyncGroup failed due to inconsistent Protocol Name, received {} but expected {}",
                                    protocolName, generation.protocolName);

                                future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                            } else {
                                log.info("Successfully synced group in generation {}", generation);
                                state = MemberState.STABLE;
                                rejoinNeeded = false;
                                // record rebalance latency
                                lastRebalanceEndMs = time.milliseconds();
                                sensors.successfulRebalanceSensor.record(lastRebalanceEndMs - lastRebalanceStartMs);
                                lastRebalanceStartMs = -1L;

                                future.complete(ByteBuffer.wrap(syncResponse.data.assignment()));
                            }
                        } else {
                            log.info("Generation data was cleared by heartbeat thread to {} and state is now {} before " +
                                "receiving SyncGroup response, marking this rebalance as failed and retry",
                                generation, state);
                            // use ILLEGAL_GENERATION error code to let it retry immediately
                            future.raise(Errors.ILLEGAL_GENERATION);
                        }
                    }
                }
            } else {
                requestRejoin();

                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.info("SyncGroup failed: The group began another rebalance. Need to re-join the group. " +
                                 "Sent generation was {}", sentGeneration);
                    future.raise(error);
                } else if (error == Errors.FENCED_INSTANCE_ID) {
                    // for sync-group request, even if the generation has changed we would not expect the instance id
                    // gets fenced, and hence we always treat this as a fatal error
                    log.error("SyncGroup failed: The group instance id {} has been fenced by another instance. " +
                        "Sent generation was {}", rebalanceConfig.groupInstanceId, sentGeneration);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    log.info("SyncGroup failed: {} Need to re-join the group. Sent generation was {}",
                            error.message(), sentGeneration);
                    if (generationUnchanged())
                        resetGenerationOnResponseError(ApiKeys.SYNC_GROUP, error);

                    future.raise(error);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR) {
                    log.info("SyncGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                             error.message(), sentGeneration);
                    markCoordinatorUnknown(error);
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending FindCoordinator request to broker {}", node);
        // 1、构建查找Group Coordinator节点的请求
        FindCoordinatorRequest.Builder requestBuilder =
                new FindCoordinatorRequest.Builder(
                        // 设置此次请求的类型：ApiKeys.FIND_COORDINATOR
                        new FindCoordinatorRequestData()
                            .setKeyType(CoordinatorType.GROUP.id())
                            .setKey(this.rebalanceConfig.groupId));
        // 2、发送请求，并调用 FindCoordinatorResponseHandler 类对象来处理响应
        // 将请求存储在 ConsumerNetworkClient#unsent 队列中队列中
        return client.send(node, requestBuilder)
                // 调用异步请求 RequestFuture#compose() 方法为其添加监听器，
                // 并将服务端响应的回调处理器 FindCoordinatorResponseHandler 封装在监听器中。
                // 也就是说当消费者收到响应后，通过 RequestFuture#compose()方法设置
                // FindCoordinatorResponseHandler 的类对象进行处理。
                .compose(new FindCoordinatorResponseHandler());
    }

    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received FindCoordinator response {}", resp);
            clearFindCoordinatorFuture();

            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
            Errors error = findCoordinatorResponse.error();
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {
                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer

                    // 使用 MAX_VALUE - node.id 作为 Coordinator 的连接 ID，以便在底层网络客户端层为 Coordinator 与其他连接隔离
                    int coordinatorConnectionId = Integer.MAX_VALUE - findCoordinatorResponse.data().nodeId();

                    // 2、首先根据服务端响应数据构建协调器 Coordinator 节点信息
                    AbstractCoordinator.this.coordinator = new Node(
                            coordinatorConnectionId,
                            findCoordinatorResponse.data().host(),
                            findCoordinatorResponse.data().port());
                    log.info("Discovered group coordinator {}", coordinator);
                    // 3、调用 ConsumerNetworkClient#tryConnect()方法尝试与协调器 Coordinator 节点建立连接
                    client.tryConnect(coordinator);
                    // 4、重置心跳的会话超时时间
                    heartbeat.resetSessionTimeout();
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                // 如果出现 GROUP_AUTHORIZATION_FAILED 错误，则抛出 GroupAuthorizationException 异常
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                log.debug("Group coordinator lookup failed: {}", findCoordinatorResponse.data().errorMessage());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            clearFindCoordinatorFuture();
            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator() == null;
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node checkAndGetCoordinator() {
        //  coordinator 不为空且消费者与 GroupCoordinator 连接是否正常。
        if (coordinator != null && client.isUnavailable(coordinator)) {
            markCoordinatorUnknown(true, "coordinator unavailable");
            return null;
        }
        return this.coordinator;
    }

    private synchronized Node coordinator() {
        return this.coordinator;
    }


    protected synchronized void markCoordinatorUnknown(Errors error) {
        markCoordinatorUnknown(false, "error response " + error.name());
    }

    protected synchronized void markCoordinatorUnknown(String cause) {
        markCoordinatorUnknown(false, cause);
    }

    protected synchronized void markCoordinatorUnknown(boolean isDisconnected, String cause) {
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}."
                    + "isDisconnected: {}. Rediscovery will be attempted.", this.coordinator,
                    cause, isDisconnected);
            Node oldCoordinator = this.coordinator;

            // Mark the coordinator dead before disconnecting requests since the callbacks for any pending
            // requests may attempt to do likewise. This also prevents new requests from being sent to the
            // coordinator while the disconnect is in progress.
            this.coordinator = null;

            // Disconnect from the coordinator to ensure that there are no in-flight requests remaining.
            // Pending callbacks will be invoked with a DisconnectException on the next call to poll.
            if (!isDisconnected)
                client.disconnectAsync(oldCoordinator);
        }
    }

    /**
     * Get the current generation state, regardless of whether it is currently stable.
     * Note that the generation information can be updated while we are still in the middle
     * of a rebalance, after the join-group response is received.
     *
     * @return the current generation
     */
    protected synchronized Generation generation() {
        return generation;
    }

    /**
     * Get the current generation state if the group is stable, otherwise return null
     *
     * @return the current generation or null
     */
    protected synchronized Generation generationIfStable() {
        if (this.state != MemberState.STABLE)
            return null;
        return generation;
    }

    protected synchronized boolean rebalanceInProgress() {
        return this.state == MemberState.PREPARING_REBALANCE || this.state == MemberState.COMPLETING_REBALANCE;
    }

    protected synchronized String memberId() {
        return generation.memberId;
    }

    private synchronized void resetState() {
        state = MemberState.UNJOINED;
        generation = Generation.NO_GENERATION;
    }

    private synchronized void resetStateAndRejoin() {
        resetState();
        rejoinNeeded = true;
    }

    synchronized void resetGenerationOnResponseError(ApiKeys api, Errors error) {
        log.debug("Resetting generation after encountering {} from {} response and requesting re-join", error, api);

        resetStateAndRejoin();
    }

    synchronized void resetGenerationOnLeaveGroup() {
        log.debug("Resetting generation due to consumer pro-actively leaving the group");

        resetStateAndRejoin();
    }

    public synchronized void requestRejoin() {
        this.rejoinNeeded = true;
    }

    private boolean isProtocolTypeInconsistent(String protocolType) {
        return protocolType != null && !protocolType.equals(protocolType());
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public final void close() {
        close(time.timer(0));
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    protected void close(Timer timer) {
        try {
            closeHeartbeatThread();
        } finally {
            // Synchronize after closing the heartbeat thread since heartbeat thread
            // needs this lock to complete and terminate after close flag is set.
            synchronized (this) {
                if (rebalanceConfig.leaveGroupOnClose) {
                    onLeavePrepare();
                    maybeLeaveGroup("the consumer is being closed");
                }

                // At this point, there may be pending commits (async commits or sync commits that were
                // interrupted using wakeup) and the leave group request which have been queued, but not
                // yet sent to the broker. Wait up to close timeout for these pending requests to be processed.
                // If coordinator is not known, requests are aborted.
                Node coordinator = checkAndGetCoordinator();
                if (coordinator != null && !client.awaitPendingRequests(coordinator, timer))
                    log.warn("Close timed out with {} pending requests to coordinator, terminating client connections",
                            client.pendingRequestCount(coordinator));
            }
        }
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public synchronized RequestFuture<Void> maybeLeaveGroup(String leaveReason) {
        RequestFuture<Void> future = null;

        // Starting from 2.3, only dynamic members will send LeaveGroupRequest to the broker,
        // consumer with valid group.instance.id is viewed as static member that never sends LeaveGroup,
        // and the membership expiration is only controlled by session timeout.
        if (isDynamicMember() && !coordinatorUnknown() &&
            state != MemberState.UNJOINED && generation.hasMemberId()) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            log.info("Member {} sending LeaveGroup request to coordinator {} due to {}",
                generation.memberId, coordinator, leaveReason);
            LeaveGroupRequest.Builder request = new LeaveGroupRequest.Builder(
                rebalanceConfig.groupId,
                Collections.singletonList(new MemberIdentity().setMemberId(generation.memberId))
            );

            future = client.send(coordinator, request).compose(new LeaveGroupResponseHandler(generation));
            client.pollNoWakeup();
        }

        resetGenerationOnLeaveGroup();

        return future;
    }

    protected boolean isDynamicMember() {
        return !rebalanceConfig.groupInstanceId.isPresent();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        private LeaveGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            final List<MemberResponse> members = leaveResponse.memberResponses();
            if (members.size() > 1) {
                future.raise(new IllegalStateException("The expected leave group response " +
                                                           "should only contain no more than one member info, however get " + members));
            }

            final Errors error = leaveResponse.error();
            if (error == Errors.NONE) {
                log.debug("LeaveGroup response with {} returned successfully: {}", sentGeneration, response);
                future.complete(null);
            } else {
                log.error("LeaveGroup request with {} failed with error: {}", sentGeneration, error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    // HEARTBEAT 请求已就绪，存储在了 unsent 队列中
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request with generation {} and member id {} to coordinator {}",
            generation.generationId, generation.memberId, coordinator);
        // 看到这里，是不是很熟悉，前面发送 FIND_COORDINATOR 也是类似处理流程，这里心跳请求类型：ApiKeys.HEARTBEAT
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(new HeartbeatRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setGenerationId(this.generation.generationId));
        // 设置 HeartbeatResponseHandler 为心跳响应处理器,当协调器响应心跳请求时，
        // HeartbeatResponseHandler#handle() 方法将被触发执行。
        // 封装请求为 ClientRequest，在存入 unsent 队列中, coordinator是经过 FIND_COORDINATOR 请求获取到得目标 Broker 节点
        return client.send(coordinator, requestBuilder)
                .compose(new HeartbeatResponseHandler(generation));
    }

    // 心跳响应处理类
    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        // 构造函数，接收一个 Generation 对象作为参数。
        private HeartbeatResponseHandler(final Generation generation) {
            super(generation);
        }

        // 处理心跳响应的方法，接收一个 HeartbeatResponse 对象和一个 RequestFuture<Void> 对象作为参数。
        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {

            // 记录心跳请求的延迟时间。
            sensors.heartbeatSensor.record(response.requestLatencyMs());
            // 获取心跳响应的错误类型。
            Errors error = heartbeatResponse.error();


            // 如果错误类型为 Errors.NONE，表示心跳响应成功。
            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response");
                // 完成 RequestFuture 对象，表示成功完成了心跳请求。
                future.complete(null);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                // 表示协调器不可用或者不是协调器。
                log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid",
                        coordinator());
                // 标记协调器状态为未知，并传入错误类型进行标记。
                markCoordinatorUnknown(error);
                // 抛出 RequestFuture 异常，用于表示心跳请求失败。
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                // since we may be sending the request during rebalance, we should check
                // this case and ignore the REBALANCE_IN_PROGRESS error
                // 表示正在重新平衡中
                // 对 AbstractCoordinator 对象进行同步操作。
                if (state == MemberState.STABLE) {
                    log.info("Attempt to heartbeat failed since group is rebalancing");
                    // 如果状态为 MemberState.STABLE，表示成员状态已稳定。

                    // 发送重新加入群组的请求，并传入一个提示信息。
                    requestRejoin();
                    // 抛出 RequestFuture 异常，用于表示心跳请求失败。
                    future.raise(error);
                } else {
                    log.debug("Ignoring heartbeat response with error {} during {} state", error, state);
                    future.complete(null);
                }
            } else if (error == Errors.ILLEGAL_GENERATION ||
                       error == Errors.UNKNOWN_MEMBER_ID ||
                       error == Errors.FENCED_INSTANCE_ID) {
                if (generationUnchanged()) {
                    log.info("Attempt to heartbeat with {} and group instance id {} failed due to {}, resetting generation",
                        sentGeneration, rebalanceConfig.groupInstanceId, error);
                    resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                    future.raise(error);
                } else {
                    // if the generation has changed, then ignore this error
                    log.info("Attempt to heartbeat with stale {} and group instance id {} failed due to {}, ignoring the error",
                        sentGeneration, rebalanceConfig.groupInstanceId, error);
                    future.complete(null);
                }
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        CoordinatorResponseHandler(final Generation generation) {
            this.sentGeneration = generation;
        }

        final Generation sentGeneration;
        ClientResponse response;

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                markCoordinatorUnknown(true, e.getMessage());
            }
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

        boolean generationUnchanged() {
            synchronized (AbstractCoordinator.this) {
                return generation.equals(sentGeneration);
            }
        }
    }

    protected Meter createMeter(Metrics metrics, String groupName, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
                metrics.metricName(baseName + "-rate", groupName,
                        String.format("The number of %s per second", descriptiveName)),
                metrics.metricName(baseName + "-total", groupName,
                        String.format("The total number of %s", descriptiveName)));
    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatSensor;
        public final Sensor joinSensor;
        public final Sensor syncSensor;
        public final Sensor successfulRebalanceSensor;
        public final Sensor failedRebalanceSensor;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatSensor = metrics.sensor("heartbeat-latency");
            this.heartbeatSensor.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatSensor.add(createMeter(metrics, metricGrpName, "heartbeat", "heartbeats"));

            this.joinSensor = metrics.sensor("join-latency");
            this.joinSensor.add(metrics.metricName("join-time-avg",
                this.metricGrpName,
                "The average time taken for a group rejoin"), new Avg());
            this.joinSensor.add(metrics.metricName("join-time-max",
                this.metricGrpName,
                "The max time taken for a group rejoin"), new Max());
            this.joinSensor.add(createMeter(metrics, metricGrpName, "join", "group joins"));

            this.syncSensor = metrics.sensor("sync-latency");
            this.syncSensor.add(metrics.metricName("sync-time-avg",
                this.metricGrpName,
                "The average time taken for a group sync"), new Avg());
            this.syncSensor.add(metrics.metricName("sync-time-max",
                this.metricGrpName,
                "The max time taken for a group sync"), new Max());
            this.syncSensor.add(createMeter(metrics, metricGrpName, "sync", "group syncs"));

            this.successfulRebalanceSensor = metrics.sensor("rebalance-latency");
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-avg",
                this.metricGrpName,
                "The average time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Avg());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-max",
                this.metricGrpName,
                "The max time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Max());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-total",
                this.metricGrpName,
                "The total number of milliseconds this consumer has spent in successful rebalances since creation"),
                new CumulativeSum());
            this.successfulRebalanceSensor.add(
                metrics.metricName("rebalance-total",
                    this.metricGrpName,
                    "The total number of successful rebalance events, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new CumulativeCount()
            );
            this.successfulRebalanceSensor.add(
                metrics.metricName(
                    "rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of successful rebalance events per hour, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            this.failedRebalanceSensor = metrics.sensor("failed-rebalance");
            this.failedRebalanceSensor.add(
                metrics.metricName("failed-rebalance-total",
                    this.metricGrpName,
                    "The total number of failed rebalance events"),
                new CumulativeCount()
            );
            this.failedRebalanceSensor.add(
                metrics.metricName(
                    "failed-rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of failed rebalance events per hour"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            Measurable lastRebalance = (config, now) -> {
                if (lastRebalanceEndMs == -1L)
                    // if no rebalance is ever triggered, we just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-rebalance-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last successful rebalance event"),
                lastRebalance);

            Measurable lastHeartbeat = (config, now) -> {
                if (heartbeat.lastHeartbeatSend() == 0L)
                    // if no heartbeat is ever triggered, just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last coordinator heartbeat was sent"),
                lastHeartbeat);
        }
    }

    // 定义一个私有的内部类HeartbeatThread，继承自KafkaThread类，并实现AutoCloseable接口。用于实现一个心跳线程
    private class HeartbeatThread extends KafkaThread implements AutoCloseable {
        // 表示心跳线程是否启用的标志。
        private boolean enabled = false;
        // 表示心跳线程是否已关闭的标志。
        private boolean closed = false;
        // 用于保存心跳线程失败时抛出的RuntimeException异常。
        private final AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        // 构造函数，初始化心跳线程对象。
        private HeartbeatThread() {
            super(HEARTBEAT_THREAD_PREFIX + (rebalanceConfig.groupId.isEmpty() ? "" : " | " + rebalanceConfig.groupId), true);
        }

        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Enabling heartbeat thread");
                this.enabled = true;
                heartbeat.resetTimeouts();
                AbstractCoordinator.this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Disabling heartbeat thread");
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        // 重写Runnable接口中的run()方法，实现心跳线程的逻辑。
        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread started");
                while (true) {
                    // 获取 AbstractCoordinator 对象的同步锁，确保线程安全。
                    synchronized (AbstractCoordinator.this) {
                        // 如果心跳线程已关闭，则直接返回。
                        if (closed)
                            return;

                        // 如果心跳线程未启用，则线程进入等待状态。
                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        // we do not need to heartbeat we are not part of a group yet;
                        // also if we already have fatal error, the client will be
                        // crashed soon, hence we do not need to continue heartbeating either
                        // 如果当前线程尚未加入到消息分组中或者已发生失败情况，则禁用当前线程并继续下一个循环。
                        if (state.hasNotJoinedGroup() || hasFailed()) {
                            disable();
                            continue;
                        }

                        // 调用客户端的 pollNoWakeup() 方法，进行轮询消息。
                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        // 如果协调器未知，则进行协调器的查找。
                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture != null || lookupCoordinator().failed())
                                // the immediate future check ensures that we backoff properly in the case that no
                                // brokers are available to connect to.
                                AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            markCoordinatorUnknown("session timed out without receiving a "
                                    + "heartbeat response");
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll().
                            String leaveReason = "consumer poll timeout has expired. This means the time between subsequent calls to poll() " +
                                                    "was longer than the configured max.poll.interval.ms, which typically implies that " +
                                                    "the poll loop is spending too much time processing messages. " +
                                                    "You can address this either by increasing max.poll.interval.ms or by reducing " +
                                                    "the maximum size of batches returned in poll() with max.poll.records.";
                            maybeLeaveGroup(leaveReason);
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);
                            final RequestFuture<Void> heartbeatFuture = sendHeartbeatRequest();
                            heartbeatFuture.addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat();
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat();
                                        } else if (e instanceof FencedInstanceIdException) {
                                            log.error("Caught fenced group.instance.id {} error in heartbeat thread", rebalanceConfig.groupInstanceId);
                                            heartbeatThread.failed.set(e);
                                        } else {
                                            heartbeat.failHeartbeat();
                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (AuthenticationException e) {
                log.error("An authentication error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (GroupAuthorizationException e) {
                log.error("A group authorization error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread", e);
                this.failed.set(new RuntimeException(e));
            } catch (Throwable e) {
                log.error("Heartbeat thread failed due to unexpected error", e);
                if (e instanceof RuntimeException)
                    this.failed.set((RuntimeException) e);
                else
                    this.failed.set(new RuntimeException(e));
            } finally {
                log.debug("Heartbeat thread has closed");
            }
        }

    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocolName;

        public Generation(int generationId, String memberId, String protocolName) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocolName = protocolName;
        }

        /**
         * @return true if this generation has a valid member id, false otherwise. A member might have an id before
         * it becomes part of a group generation.
         */
        public boolean hasMemberId() {
            return !memberId.isEmpty();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Generation that = (Generation) o;
            return generationId == that.generationId &&
                    Objects.equals(memberId, that.memberId) &&
                    Objects.equals(protocolName, that.protocolName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generationId, memberId, protocolName);
        }

        @Override
        public String toString() {
            return "Generation{" +
                    "generationId=" + generationId +
                    ", memberId='" + memberId + '\'' +
                    ", protocol='" + protocolName + '\'' +
                    '}';
        }
    }

    @SuppressWarnings("serial")
    private static class UnjoinedGroupException extends RetriableException {

    }

    // For testing only below
    final Heartbeat heartbeat() {
        return heartbeat;
    }

    final synchronized void setLastRebalanceTime(final long timestamp) {
        lastRebalanceEndMs = timestamp;
    }

    /**
     * Check whether given generation id is matching the record within current generation.
     *
     * @param generationId generation id
     * @return true if the two ids are matching.
     */
    final boolean hasMatchingGenerationId(int generationId) {
        return !generation.equals(Generation.NO_GENERATION) && generation.generationId == generationId;
    }

    final boolean hasUnknownGeneration() {
        return generation.equals(Generation.NO_GENERATION);
    }

    /**
     * @return true if the current generation's member ID is valid, false otherwise
     */
    final boolean hasValidMemberId() {
        return !hasUnknownGeneration() && generation.hasMemberId();
    }

    final synchronized void setNewGeneration(final Generation generation) {
        this.generation = generation;
    }

    final synchronized void setNewState(final MemberState state) {
        this.state = state;
    }
}
