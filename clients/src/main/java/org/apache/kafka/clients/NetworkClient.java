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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.CommonFields;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    // 状态枚举值
    private enum State {
        ACTIVE,
        CLOSING,
        CLOSED
    }

    private final Logger log;

    /* the selector used to perform network i/o */
    // 用于执行网络 I/O 的选择器
    private final Selectable selector;

    // Metadata元信息的更新器, 它可以尝试更新元信息
    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    // 管理集群所有节点连接的状态
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response */
    // 当前正在发送或等待响应的请求集合
    private final InFlightRequests inFlightRequests;

    /* the socket send buffer size in bytes */
    // 套接字发送数据的缓冲区的大小（以字节为单位）
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    // 套接字接收数据的缓冲区的大小（以字节为单位）
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    // 表示客户端id，标识客户端身份
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    // 向服务器发送请求时使用的当前关联 ID
    private int correlation;

    /* default timeout for individual requests to await acknowledgement from servers */
    // 单个请求等待服务器确认的默认超时
    private final int defaultRequestTimeoutMs;

    /* time in ms to wait before retrying to create connection to a server */
    // 重连的退避时间
    private final long reconnectBackoffMs;

    private final ClientDnsLookup clientDnsLookup;

    private final Time time;

    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     */
    // 是否需要与 Broker 端的版本协调，默认为 true
    // 如果为 true 当第一次连接到一个 broker 时，应当发送一个 version 的请求，
    // 用来得知 broker 的版本， 如果为 false 则不需要发送 version 的请求。
    private final boolean discoverBrokerVersions;

    // broker 端版本
    private final ApiVersions apiVersions;

    // 存储着要发送的版本请求，key 为 nodeId，value 为构建请求的 Builder
    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();

    // 取消的请求集合
    private final List<ClientResponse> abortedSends = new LinkedList<>();

    private final Sensor throttleTimeSensor;

    private final AtomicReference<State> state;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         ClientDnsLookup clientDnsLookup,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(null,
             metadata,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             clientDnsLookup,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext);
    }

    public NetworkClient(Selectable selector,
            Metadata metadata,
            String clientId,
            int maxInFlightRequestsPerConnection,
            long reconnectBackoffMs,
            long reconnectBackoffMax,
            int socketSendBuffer,
            int socketReceiveBuffer,
            int defaultRequestTimeoutMs,
            long connectionSetupTimeoutMs,
            long connectionSetupTimeoutMaxMs,
            ClientDnsLookup clientDnsLookup,
            Time time,
            boolean discoverBrokerVersions,
            ApiVersions apiVersions,
            Sensor throttleTimeSensor,
            LogContext logContext) {
        this(null,
             metadata,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             clientDnsLookup,
             time,
             discoverBrokerVersions,
             apiVersions,
             throttleTimeSensor,
             logContext);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         ClientDnsLookup clientDnsLookup,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext) {
        this(metadataUpdater,
             null,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             clientDnsLookup,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext);
    }

    private NetworkClient(MetadataUpdater metadataUpdater,
                          Metadata metadata,
                          Selectable selector,
                          String clientId,
                          int maxInFlightRequestsPerConnection,
                          long reconnectBackoffMs,
                          long reconnectBackoffMax,
                          int socketSendBuffer,
                          int socketReceiveBuffer,
                          int defaultRequestTimeoutMs,
                          long connectionSetupTimeoutMs,
                          long connectionSetupTimeoutMaxMs,
                          ClientDnsLookup clientDnsLookup,
                          Time time,
                          boolean discoverBrokerVersions,
                          ApiVersions apiVersions,
                          Sensor throttleTimeSensor,
                          LogContext logContext) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            // 通过这里可以看出 metadataUpdate 实例化对象
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(
                reconnectBackoffMs, reconnectBackoffMax,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, logContext);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
        this.apiVersions = apiVersions;
        this.throttleTimeSensor = throttleTimeSensor;
        this.log = logContext.logger(NetworkClient.class);
        this.clientDnsLookup = clientDnsLookup;
        this.state = new AtomicReference<>(State.ACTIVE);
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    public boolean ready(Node node, long now) {
        // 空节点
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        // 1、判断节点是否准备好发送请求
        if (isReady(node, now))
            return true;

        // 2、判断节点连接状态
        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            // 3、初始化连接，但此时不一定连接成功了
            initiateConnect(node, now);

        return false;
    }

    // Visible for testing
    boolean canConnect(Node node, long now) {
        return connectionStates.canConnect(node.idString(), now);
    }

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     */
    @Override
    public void disconnect(String nodeId) {
        if (connectionStates.isDisconnected(nodeId))
            return;

        selector.close(nodeId);
        long now = time.milliseconds();

        cancelInFlightRequests(nodeId, now, abortedSends);

        connectionStates.disconnected(nodeId, now);

        if (log.isTraceEnabled()) {
            log.trace("Manually disconnected from {}. Aborted in-flight requests: {}.", nodeId, inFlightRequests);
        }
    }

    private void cancelInFlightRequests(String nodeId, long now, Collection<ClientResponse> responses) {
        Iterable<InFlightRequest> inFlightRequests = this.inFlightRequests.clearAll(nodeId);
        for (InFlightRequest request : inFlightRequests) {
            log.trace("Cancelled request {} {} with correlation id {} due to node {} being disconnected",
                    request.header.apiKey(), request.request, request.header.correlationId(), nodeId);

            if (!request.isInternalRequest) {
                if (responses != null)
                    responses.add(request.disconnected(now, null));
            } else if (request.header.apiKey() == ApiKeys.METADATA) {
                metadataUpdater.handleFailedRequest(now, Optional.empty());
            }
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        selector.close(nodeId);
        long now = time.milliseconds();
        cancelInFlightRequests(nodeId, now, null);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    // Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
    // This is for testing.
    public long throttleDelayMs(Node node, long now) {
        return connectionStates.throttleDelayMs(node.idString(), now);
    }

    /**
     * Return the poll delay in milliseconds based on both connection and throttle delay.
     * @param node the connection to check
     * @param now the current time in ms
     */
    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionStates.pollDelayMs(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.isDisconnected(node.idString());
    }

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    @Override
    public AuthenticationException authenticationException(Node node) {
        return connectionStates.authenticationException(node.idString());
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        // 当发现正在更新元数据时，会禁止发送请求 && 当连接没有创建完毕或者当前发送的请求过多时，也会禁止发送请求
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString(), now);
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     * @param now the current timestamp
     */
    // 检测连接状态、发送请求是否过多
    private boolean canSendRequest(String node, long now) {
        //  // 三个条件必须都满足
        return connectionStates.isReady(node, now) && selector.isChannelReady(node) &&
            inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * @param request The request
     * @param now The current timestamp
     */
    // 发送请求，这个方法 生产者和消费者都会调用，其中 ClientRequest 表示客户端的请求。
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    // package-private for testing
    void sendInternalMetadataRequest(MetadataRequest.Builder builder, String nodeConnectionId, long now) {
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);
        doSend(clientRequest, true, now);
    }

    // 检测请求版本是否支持，如果支持则发送请求
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        // 确认是否活跃
        ensureActive();
        // 目标节点id
        String nodeId = clientRequest.destination();
        // 是否是 NetworkClient 内部请求 这里为 false
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            // 检测是否可以向指定 Node 发送请求，如果还不能发送请求则抛异常
            if (!canSendRequest(nodeId, now))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            // 检测版本
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            short version;
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                version = builder.latestAllowedVersion();
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
            } else {
                version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.

            // builder.build()是 ProduceRequest.Builder，结果是ProduceRequest
            // 调用 doSend 方法
            doSend(clientRequest, isInternalRequest, now, builder.build(version));
        } catch (UnsupportedVersionException unsupportedVersionException) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug("Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                    clientRequest.correlationId(), clientRequest.destination(), unsupportedVersionException);
            // 请求的版本不协调，那么生成 clientResponse
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, unsupportedVersionException, null, null);

            if (!isInternalRequest)
                // 添加到 abortedSends 集合里
                abortedSends.add(clientResponse);
            else if (clientRequest.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleFailedRequest(now, Optional.of(unsupportedVersionException));
        }
    }

    // isInternalRequest 表示发送前是否需要验证连接状态，如果为 true 则表示客户端已经确定连接是好的
    // request表示请求体
    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        // 目标节点地址
        String destination = clientRequest.destination();
        // 生成请求头
        RequestHeader header = clientRequest.makeHeader(request.version());
        if (log.isDebugEnabled()) {
            log.debug("Sending {} request with header {} and timeout {} to node {}: {}",
                clientRequest.apiKey(), header, clientRequest.requestTimeoutMs(), destination, request);
        }
        // 1、构建 NetworkSend 对象 结合请求头和请求体，序列化数据，保存到 NetworkSend
        Send send = request.toSend(destination, header);
        // 2、构建 inFlightRequest 对象 保存了发送前的所有信息
        InFlightRequest inFlightRequest = new InFlightRequest(
                clientRequest,
                header,
                isInternalRequest,
                request,
                send,
                now);
        // 3、把 inFlightRequest 加入 inFlightRequests 集合里
        this.inFlightRequests.add(inFlightRequest);
        // 4、调用 Selector 异步发送数据，并将 send 和对应 kafkaChannel 绑定起来，
        // 并开启该 kafkaChannel 底层 socket 的写事件，等待下一步真正的网络发送
        selector.send(send);
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        // 确认是否活跃
        ensureActive();

        // 取消发送是否为空
        if (!abortedSends.isEmpty()) {
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.
            List<ClientResponse> responses = new ArrayList<>();
            handleAbortedSends(responses);
            completeResponses(responses);
            return responses;
        }

        // 尝试更新元数据
        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        try {
            // 执行IO操作
            this.selector.poll(Utils.min(timeout, metadataTimeout, defaultRequestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        long updatedNow = this.time.milliseconds();
        // 响应结果集合：真正的读写操作, 会生成responses
        List<ClientResponse> responses = new ArrayList<>();
        // 3、完成发送的handler，处理 completedSends 集合
        handleCompletedSends(responses, updatedNow);
        // 4、完成接收的handler，处理 completedReceives 队列
        handleCompletedReceives(responses, updatedNow);
        // 5、断开连接的handler，处理 disconnected 列表
        handleDisconnections(responses, updatedNow);
        // 6、处理连接的handler，处理 connected 列表
        handleConnections();
        // 7、处理版本协调请求（获取api版本号） handler
        handleInitiateApiVersionRequests(updatedNow);
        // 8、超时连接的handler，处理超时连接集合
        handleTimedOutConnections(responses, updatedNow);
        // 9、超时请求的handler，处理超时请求集合
        handleTimedOutRequests(responses, updatedNow);
        // 10、完成响应回调
        completeResponses(responses);

        return responses;
    }

    private void completeResponses(List<ClientResponse> responses) {
        // 遍历响应结果集合
        for (ClientResponse response : responses) {
            try {
                // 调用响应完成的回调函数
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.count();
    }

    @Override
    public boolean hasInFlightRequests() {
        return !this.inFlightRequests.isEmpty();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.count(node);
    }

    @Override
    public boolean hasInFlightRequests(String node) {
        return !this.inFlightRequests.isEmpty(node);
    }

    @Override
    public boolean hasReadyNodes(long now) {
        return connectionStates.hasReadyNodes(now);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    @Override
    public void initiateClose() {
        if (state.compareAndSet(State.ACTIVE, State.CLOSING)) {
            wakeup();
        }
    }

    @Override
    public boolean active() {
        // 判断状态是否是活跃的
        return state.get() == State.ACTIVE;
    }

    // 确认是否活跃
    private void ensureActive() {
        if (!active())
            throw new DisconnectException("NetworkClient is no longer active, state is " + state);
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        state.compareAndSet(State.ACTIVE, State.CLOSING);
        if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
            this.selector.close();
            this.metadataUpdater.close();
        } else {
            log.warn("Attempting to close NetworkClient that has already been closed.");
        }
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. If no connection exists, this method will prefer a node
     * with least recent connection attempts. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period, or an active
     * connection which is being throttled.
     *
     * @return The node with the fewest in-flight requests.
     */
    @Override
    public Node leastLoadedNode(long now) {
        // 从元数据中获取所有的节点
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        if (nodes.isEmpty())
            throw new IllegalStateException("There are no nodes in the Kafka cluster");
        int inflight = Integer.MAX_VALUE;

        Node foundConnecting = null;
        Node foundCanConnect = null;
        Node foundReady = null;

        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);
            // 节点是否可以发送请求
            if (canSendRequest(node.idString(), now)) {
                // 获取节点的队列大小
                int currInflight = this.inFlightRequests.count(node.idString());
                // 如果为 0 则返回该节点，负载最小
                if (currInflight == 0) {
                    // if we find an established connection with no in-flight requests we can stop right away
                    log.trace("Found least loaded node {} connected with no in-flight requests", node);
                    return node;
                } else if (currInflight < inflight) {
                    // 如果队列大小小于最大值
                    // otherwise if this is the best we have found so far, record that
                    inflight = currInflight;
                    foundReady = node;
                }
            } else if (connectionStates.isPreparingConnection(node.idString())) {
                foundConnecting = node;
            } else if (canConnect(node, now)) {
                if (foundCanConnect == null ||
                        this.connectionStates.lastConnectAttemptMs(foundCanConnect.idString()) >
                                this.connectionStates.lastConnectAttemptMs(node.idString())) {
                    foundCanConnect = node;
                }
            } else {
                log.trace("Removing node {} from least loaded node selection since it is neither ready " +
                        "for sending or connecting", node);
            }
        }

        // We prefer established connections if possible. Otherwise, we will wait for connections
        // which are being established before connecting to new nodes.
        if (foundReady != null) {
            log.trace("Found least loaded node {} with {} inflight requests", foundReady, inflight);
            return foundReady;
        } else if (foundConnecting != null) {
            log.trace("Found least loaded connecting node {}", foundConnecting);
            return foundConnecting;
        } else if (foundCanConnect != null) {
            log.trace("Found least loaded node {} with no active connection", foundCanConnect);
            return foundCanConnect;
        } else {
            log.trace("Least loaded node selection failed to find an available node");
            return null;
        }
    }

    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        try {
            Struct responseStruct = parseStructMaybeUpdateThrottleTimeMetrics(responseBuffer, requestHeader, null, 0);
            return AbstractResponse.parseResponse(requestHeader.apiKey(), responseStruct,
                    requestHeader.apiVersion());
        } catch (BufferUnderflowException e) {
            throw new SchemaException("Buffer underflow while parsing response for request with header " + requestHeader, e);
        }
    }

    private static Struct parseStructMaybeUpdateThrottleTimeMetrics(ByteBuffer responseBuffer, RequestHeader requestHeader,
                                                                    Sensor throttleTimeSensor, long now) {
        // 获取响应头
        ResponseHeader responseHeader = ResponseHeader.parse(responseBuffer,
            requestHeader.apiKey().responseHeaderVersion(requestHeader.apiVersion()));
        // Always expect the response version id to be the same as the request version id
        // 获取响应体
        Struct responseBody = requestHeader.apiKey().parseResponse(requestHeader.apiVersion(), responseBuffer);
        // 对比响应头 correlationId 和响应体的 correlationId 是否一致，否则抛异常
        correlate(requestHeader, responseHeader);
        if (throttleTimeSensor != null && responseBody.hasField(CommonFields.THROTTLE_TIME_MS))
            throttleTimeSensor.record(responseBody.get(CommonFields.THROTTLE_TIME_MS), now);
        return responseBody;
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     * @param disconnectState The state of the disconnected channel
     */
    // 处理断开连接
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState) {
        // 处理节点连接状态为 DISCONNECTED，并更新超时时间和重试退避时间
        connectionStates.disconnected(nodeId, now);
        apiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId);
        // 判断状态
        switch (disconnectState.state()) {
            // 授权失败
            case AUTHENTICATION_FAILED:
                AuthenticationException exception = disconnectState.exception();
                connectionStates.authenticationFailed(nodeId, now, exception);
                log.error("Connection to node {} ({}) failed authentication due to: {}", nodeId,
                    disconnectState.remoteAddress(), exception.getMessage());
                break;
            case AUTHENTICATE:
                log.warn("Connection to node {} ({}) terminated during authentication. This may happen " +
                    "due to any of the following reasons: (1) Authentication failed due to invalid " +
                    "credentials with brokers older than 1.0.0, (2) Firewall blocking Kafka TLS " +
                    "traffic (eg it may only allow HTTPS traffic), (3) Transient network issue.",
                    nodeId, disconnectState.remoteAddress());
                break;
            // 未连接
            case NOT_CONNECTED:
                log.warn("Connection to node {} ({}) could not be established. Broker may not be available.", nodeId, disconnectState.remoteAddress());
                break;
            default:
                break; // Disconnections in other states are logged at debug level in Selector
        }

        // 取消请求
        cancelInFlightRequests(nodeId, now, responses);
        // 将连接失败通知元数据更新器
        metadataUpdater.handleServerDisconnect(now, nodeId, Optional.ofNullable(disconnectState.exception()));
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    // 处理超时请求
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        // 获取超时请求的节点list
        List<String> nodeIds = this.inFlightRequests.nodesWithTimedOutRequests(now);
        // 获取超时请求的节点list
        for (String nodeId : nodeIds) {
            // close connection to the node
            // 先关闭节点连接器
            this.selector.close(nodeId);
            log.debug("Disconnecting from node {} due to request timeout.", nodeId);
            // 处理断开连接
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);
        }
    }

    private void handleAbortedSends(List<ClientResponse> responses) {
        responses.addAll(abortedSends);
        abortedSends.clear();
    }

    /**
     * Handle socket channel connection timeout. The timeout will hit iff a connection
     * stays at the ConnectionState.CONNECTING state longer than the timeout value,
     * as indicated by ClusterConnectionStates.NodeConnectionState.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    // 处理超时连接
    private void handleTimedOutConnections(List<ClientResponse> responses, long now) {
        // 获取超时连接的节点集合
        Set<String> nodes = connectionStates.nodesWithConnectionSetupTimeout(now);
        for (String nodeId : nodes) {
            // 先关闭节点连接器
            this.selector.close(nodeId);
            log.debug(
                "Disconnecting from node {} due to socket connection setup timeout. " +
                "The timeout value is {} ms.",
                nodeId,
                connectionStates.connectionSetupTimeoutMs(nodeId));
            // 处理断开连接
            processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE);
        }
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        // 上面发送完成将 send 添加到 completedSends 集合，然后遍历这个集合
        for (Send send : this.selector.completedSends()) {
            // 获取 inFlightRequests 集合发往对应 Broker 的最后一个请求元素
            InFlightRequest request = this.inFlightRequests.lastSent(send.destination());
            // 判断是否期望进行响应
            if (!request.expectResponse) {
                // 如果不期望进行响应就删除inFlightRequests集合发往对应 Broker 请求队列的第一个元素
                this.inFlightRequests.completeLastSent(send.destination());
                // 把请求添加到 responses 集合里
                responses.add(request.completed(null, now));
            }
        }
    }

    /**
     * If a response from a node includes a non-zero throttle delay and client-side throttling has been enabled for
     * the connection to the node, throttle the connection for the specified delay.
     *
     * @param response the response
     * @param apiVersion the API version of the response
     * @param nodeId the id of the node
     * @param now The current time
     */
    private void maybeThrottle(AbstractResponse response, short apiVersion, String nodeId, long now) {
        int throttleTimeMs = response.throttleTimeMs();
        if (throttleTimeMs > 0 && response.shouldClientThrottle(apiVersion)) {
            connectionStates.throttle(nodeId, now + throttleTimeMs);
            log.trace("Connection to node {} is throttled for {} ms until timestamp {}", nodeId, throttleTimeMs,
                      now + throttleTimeMs);
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    // 处理 CompletedReceives 队列,根据返回的响应信息实例化 ClientResponse ,并加到响应集合里
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        // 当读完后把这个 NetworkReceive 对象添加到已经接收完毕网络请求集合里，然后遍历这个集合
        for (NetworkReceive receive : this.selector.completedReceives()) {
            // 获取发送请求的node id
            String source = receive.source();
            // 从 InFlightRequest 集合取出对应的元素并删除
            InFlightRequest req = inFlightRequests.completeNext(source);
            // 解析该响应 并且验证响应头，生成 responseStruct 实例
            Struct responseStruct = parseStructMaybeUpdateThrottleTimeMetrics(receive.payload(), req.header,
                throttleTimeSensor, now);
            // 生成响应体
            AbstractResponse response = AbstractResponse.
                parseResponse(req.header.apiKey(), responseStruct, req.header.apiVersion());

            if (log.isDebugEnabled()) {
                log.debug("Received {} response from node {} for request with header {}: {}",
                    req.header.apiKey(), req.destination, req.header, response);
            }

            // If the received response includes a throttle delay, throttle the connection.
            // 流控处理
            maybeThrottle(response, req.header.apiVersion(), req.destination, now);
            // 判断返回类型
            if (req.isInternalRequest && response instanceof MetadataResponse)
                // 如果是MetadataResponse类的响应，由metadataUpdater来处理

                // 处理成功返回的响应信息
                metadataUpdater.handleSuccessfulResponse(req.header, now, (MetadataResponse) response);
            else if (req.isInternalRequest && response instanceof ApiVersionsResponse)
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) response);
            else
                // 添加响应到响应结果集合中
                responses.add(req.completed(response, now));
        }
    }

    // 处理版本协调响应
    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        // 目标节点
        final String node = req.destination;
        // 判断响应和版本是否协调
        if (apiVersionsResponse.data.errorCode() != Errors.NONE.code()) {
            // 处理响应异常
            if (req.request.version() == 0 || apiVersionsResponse.data.errorCode() != Errors.UNSUPPORTED_VERSION.code()) {
                log.warn("Received error {} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                        Errors.forCode(apiVersionsResponse.data.errorCode()), node, req.header.correlationId());
                // 关闭节点连接
                this.selector.close(node);
                // 处理断开连接
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE);
            } else {
                // Starting from Apache Kafka 2.4, ApiKeys field is populated with the supported versions of
                // the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
                // If not provided, the client falls back to version 0.
                short maxApiVersion = 0;
                if (apiVersionsResponse.data.apiKeys().size() > 0) {
                    ApiVersionsResponseKey apiVersion = apiVersionsResponse.data.apiKeys().find(ApiKeys.API_VERSIONS.id);
                    if (apiVersion != null) {
                        maxApiVersion = apiVersion.maxVersion();
                    }
                }
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder(maxApiVersion));
            }
            return;
        }
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(apiVersionsResponse.data.apiKeys());
        // 更新版本
        apiVersions.update(node, nodeVersionInfo);
        // 更新连接状态为 ready，可以正常发送请求了。
        this.connectionStates.ready(node);
        log.debug("Recorded API versions for node {}: {}", node, nodeVersionInfo);
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        // 遍历断开连接的 broker 集合数据
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            // 获取节点
            String node = entry.getKey();
            log.debug("Node {} disconnected.", node);
            // 处理断开连接
            processDisconnection(responses, node, now, entry.getValue());
        }
    }

    /**
     * Record any newly completed connections
     */
    // 处理连接的handler
    private void handleConnections() {
        // 遍历 selector 的刚刚连接成功的集合
        for (String node : this.selector.connected()) {
            // We are now connected.  Note that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            if (discoverBrokerVersions) {
                // 更新连接的状态为版本协调状态
                this.connectionStates.checkingApiVersions(node);
                // 将请求保存到 nodesNeedingApiVersionsFetch 集合里
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {
                // 更新连接的状态为连接状态
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }

    // 发送版本协调请求
    private void handleInitiateApiVersionRequests(long now) {
        // 遍历请求集合 nodesNeedingApiVersionsFetch, 在调用 handleConnections 时被加入的
        Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
            String node = entry.getKey();
            // 判断是否允许发送请求
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();
                // 调用 newClientRequest 生成请求
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);
                // 发送请求
                doSend(clientRequest, true, now);
                // 完成后移除
                iter.remove();
            }
        }
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     */
    private static void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId()) {
            if (SaslClientAuthenticator.isReserved(requestHeader.correlationId())
                    && !SaslClientAuthenticator.isReserved(responseHeader.correlationId()))
                throw new SchemaException("the response is unrelated to Sasl request since its correlation id is " + responseHeader.correlationId()
                    + " and the reserved range for Sasl request is [ "
                    + SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + "," + SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + "]");
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + "), request header: " + requestHeader);
        }
    }

    /**
     * Initiate a connection to the given node
     * @param node the node to connect to
     * @param now current time in epoch milliseconds
     */
    // 创建连接
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            // 1、更新连接状态为正在连接
            connectionStates.connecting(nodeConnectionId, now, node.host(), clientDnsLookup);
            // 获取连接地址
            InetAddress address = connectionStates.currentAddress(nodeConnectionId);
            log.debug("Initiating connection to node {} using address {}", node, address);
            // 2、调用 selector 尝试异步进行连接，后续通过selector.poll进行监听事件就绪
            selector.connect(nodeConnectionId,
                    new InetSocketAddress(address, node.port()),
                    this.socketSendBuffer,
                    this.socketReceiveBuffer);
        } catch (IOException e) {
            log.warn("Error connecting to node {}", node, e);
            // Attempt failed, we'll try again after the backoff
            connectionStates.disconnected(nodeConnectionId, now);
            // Notify metadata updater of the connection failure
            metadataUpdater.handleServerDisconnect(now, nodeConnectionId, Optional.empty());
        }
    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        private final Metadata metadata;

        // Defined if there is a request in progress, null otherwise
        private InProgressData inProgress;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.inProgress = null;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return !hasFetchInProgress() && this.metadata.timeToNextUpdate(now) == 0;
        }

        private boolean hasFetchInProgress() {
            return inProgress != null;
        }

        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            // 计算下次要更新元数据的时间，其中会检测needUpdate的值、退避时间、是否长时间未更新
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            // 检测是否已经发送了元数据的请求，如果一个元数据的请求，还未从服务端返回，那么时间设置为 waitForMetadataFetch（默认30s）
            long waitForMetadataFetch = hasFetchInProgress() ? defaultRequestTimeoutMs : 0;

            // 计算元数据超时时间
            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
            if (metadataTimeout > 0) {
                return metadataTimeout;
            }

            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            // 表示需要立即更新，取最空闲的节点node
            Node node = leastLoadedNode(now);
            if (node == null) {
                log.debug("Give up sending metadata request since no node is available");
                return reconnectBackoffMs;
            }

            // 发送更新元数据的请求
            return maybeUpdate(now, node);
        }

        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
            Cluster cluster = metadata.fetch();
            // 'processDisconnection' generates warnings for misconfigured bootstrap server configuration
            // resulting in 'Connection Refused' and misconfigured security resulting in authentication failures.
            // The warning below handles the case where a connection to a broker was established, but was disconnected
            // before metadata could be obtained.
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destinationId);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {} disconnected", node);
            }

            // If we have a disconnect while an update is due, we treat it as a failed update
            // so that we can backoff properly
            if (isUpdateDue(now))
                handleFailedRequest(now, Optional.empty());

            maybeFatalException.ifPresent(metadata::fatalError);

            // The disconnect may be the result of stale metadata, so request an update
            metadata.requestUpdate();
        }

        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            maybeFatalException.ifPresent(metadata::fatalError);
            metadata.failedUpdate(now);
            inProgress = null;
        }

        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            // If any partition has leader with missing listeners, log up to ten of these partitions
            // for diagnosing broker configuration issues.
            // This could be a transient issue if listeners were added dynamically to brokers.
            List<TopicPartition> missingListenerPartitions = response.topicMetadata().stream().flatMap(topicMetadata ->
                topicMetadata.partitionMetadata().stream()
                    .filter(partitionMetadata -> partitionMetadata.error == Errors.LISTENER_NOT_FOUND)
                    .map(partitionMetadata -> new TopicPartition(topicMetadata.topic(), partitionMetadata.partition())))
                .collect(Collectors.toList());
            if (!missingListenerPartitions.isEmpty()) {
                int count = missingListenerPartitions.size();
                log.warn("{} partitions have leader brokers without a matching listener, including {}",
                        count, missingListenerPartitions.subList(0, Math.min(10, count)));
            }

            // Check if any topic's metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            // 如果返回错误，直接报错
            if (!errors.isEmpty())
                log.warn("Error while fetching metadata with correlation id {} : {}", requestHeader.correlationId(), errors);

            // Don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
            // created which means we will get errors and no nodes until it exists
            // 判断broker信息是否为空，如果为空表示没有获得元数据
            if (response.brokers().isEmpty()) {
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                // 更新失败
                this.metadata.failedUpdate(now);
            } else {
                // 如果成功 则开始更新元数据
                this.metadata.update(inProgress.requestVersion, response, inProgress.isPartialUpdate, now);
            }

            inProgress = null;
        }

        @Override
        public void close() {
            this.metadata.close();
        }

        /**
         * Return true if there's at least one connection establishment is currently underway
         */
        private boolean isAnyNodeConnecting() {
            for (Node node : fetchNodes()) {
                if (connectionStates.isConnecting(node.idString())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         */
        private long maybeUpdate(long now, Node node) {
            // 判断当前node的状态是否可以发送Request请求
            String nodeConnectionId = node.idString();

            if (canSendRequest(nodeConnectionId, now)) {
                // 构建元数据请求
                Metadata.MetadataRequestAndVersion requestAndVersion = metadata.newMetadataRequestAndVersion(now);
                MetadataRequest.Builder metadataRequest = requestAndVersion.requestBuilder;
                log.debug("Sending metadata request {} to node {}", metadataRequest, node);
                // 向 nodeConnectionId 发送元数据请求
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);
                inProgress = new InProgressData(requestAndVersion.requestVersion, requestAndVersion.isPartialUpdate);
                return defaultRequestTimeoutMs;
            }

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            // 判断Node是否正在连接
            if (isAnyNodeConnecting()) {
                // Strictly the timeout we should return here is "connect timeout", but as we don't
                // have such application level configuration, using reconnect backoff instead.
                return reconnectBackoffMs;
            }

            // 如果存在可用的Node，则尝试初始化连接
            if (connectionStates.canConnect(nodeConnectionId, now)) {
                // We don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node);
                // 初始化与node的连接
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // connected, but can't send more OR connecting
            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            // 阻塞等待有新的节点可用
            return Long.MAX_VALUE;
        }

        public class InProgressData {
            public final int requestVersion;
            public final boolean isPartialUpdate;

            private InProgressData(int requestVersion, boolean isPartialUpdate) {
                this.requestVersion = requestVersion;
                this.isPartialUpdate = isPartialUpdate;
            }
        };

    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, defaultRequestTimeoutMs, null);
    }

    // visible for testing
    int nextCorrelationId() {
        if (SaslClientAuthenticator.isReserved(correlation)) {
            // the numeric overflow is fine as negative values is acceptable
            correlation = SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + 1;
        }
        return correlation++;
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse,
                                          int requestTimeoutMs,
                                          RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, nextCorrelationId(), clientId, createdTimeMs, expectResponse,
                requestTimeoutMs, callback);
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }

    static class InFlightRequest {
        //  请求头
        final RequestHeader header;
        // 这个请求要发送到哪个 Broker 节点上
        final String destination;
        // 回调函数
        final RequestCompletionHandler callback;
        // 是否需要进行响应
        final boolean expectResponse;
        // 请求体
        final AbstractRequest request;
        // 发送前是否需要验证连接状态
        final boolean isInternalRequest; // used to flag requests which are initiated internally by NetworkClient
        // 请求的序列化数据
        final Send send;
        // 发送时间
        final long sendTimeMs;
        // 请求的创建时间，即 ClientRequest 的创建时间
        final long createdTimeMs;
        // 请求超时时间
        final long requestTimeoutMs;

        public InFlightRequest(ClientRequest clientRequest,
                               RequestHeader header,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this(header,
                 clientRequest.requestTimeoutMs(),
                 clientRequest.createdTimeMs(),
                 clientRequest.destination(),
                 clientRequest.callback(),
                 clientRequest.expectResponse(),
                 isInternalRequest,
                 request,
                 send,
                 sendTimeMs);
        }

        public InFlightRequest(RequestHeader header,
                               int requestTimeoutMs,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.requestTimeoutMs = requestTimeoutMs;
            this.createdTimeMs = createdTimeMs;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.request = request;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
        }

        // 收到响应，回调的时候据响应内容生成 ClientResponse
        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    false, null, null, response);
        }

        // 当连接突然断开，也会生成 ClientResponse。
        public ClientResponse disconnected(long timeMs, AuthenticationException authenticationException) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, null, authenticationException, null);
        }

        @Override
        public String toString() {
            return "InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")";
        }
    }

}
