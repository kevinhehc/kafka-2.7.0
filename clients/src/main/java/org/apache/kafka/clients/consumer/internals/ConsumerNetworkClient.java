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

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Higher level consumer access to the network layer with basic support for request futures. This class
 * is thread-safe, but provides no synchronization for response callbacks. This guarantees that no locks
 * are held when they are invoked.
 */
public class ConsumerNetworkClient implements Closeable {
    // 一次拉取最大的超时时间 5 秒
    private static final int MAX_POLL_TIMEOUT_MS = 5000;

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    private final Logger log;
    // NetworkClient 对象
    private final KafkaClient client;
    // 缓冲队列，其中 key 是 Node 节点，value 是发往此 Node 的 ClientRequest 集合。
    private final UnsentRequests unsent = new UnsentRequests();
    // 消费者端 Kafka 集群元数据
    private final Metadata metadata;
    private final Time time;
    // 重试退避时间
    private final long retryBackoffMs;
    // 超时时间，默认 5000 ms
    private final int maxPollTimeoutMs;
    // 请求超时时间，默认 30000 ms
    private final int requestTimeoutMs;
    // 是否禁用 wakeup()，因为有的方法已经执行的情况下就没有必要再唤醒 Selector.poll() 方法的阻塞了，
    // 比如 close() 方法，因为网络 I/O 已经要关闭了就没必要再唤醒。
    private final AtomicBoolean wakeupDisabled = new AtomicBoolean();

    // We do not need high throughput, so use a fair lock to try to avoid starvation
    // ReentrantLock 类的对象，用来保证类的线程安全。
    private final ReentrantLock lock = new ReentrantLock(true);

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for deadlocks.

    // 用来存储请求的回调对象的队列集合。响应回来后并不是直接调用回调对象里的回调方法，而是放到队列中等响应处理完了统一调用回调方法。
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    // 用来存储待断开网络连接的节点集合。
    private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    // 是否唤醒消费者网络客户端，当有唤醒 selector.poll() 阻塞的操作时，就把这个值设为 true。
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

    public ConsumerNetworkClient(LogContext logContext,
                                 KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 int requestTimeoutMs,
                                 int maxPollTimeoutMs) {
        this.log = logContext.logger(ConsumerNetworkClient.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int defaultRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    /**
     * Send a request with the default timeout. See {@link #send(Node, AbstractRequest.Builder, int)}.
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        return send(node, requestBuilder, requestTimeoutMs);
    }

    /**
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(Timer)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     *
     * @param node The destination of the request
     * @param requestBuilder A builder for the request payload
     * @param requestTimeoutMs Maximum time in milliseconds to await a response before disconnecting the socket and
     *                         cancelling the request. The request may be cancelled sooner if the socket disconnects
     *                         for any reason.
     * @return A future which indicates the result of the send.
     */
    public RequestFuture<ClientResponse> send(Node node,
                                              AbstractRequest.Builder<?> requestBuilder,
                                              int requestTimeoutMs) {
        long now = time.milliseconds();
        // 1、新建一个 RequestFutureCompletionHandler 对象作为请求完成的处理器
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        // 2、构造请求对象，调用 NetworkClient#newClientRequest() 方法生成一个网络 ClientRequest 对象
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
                requestTimeoutMs, completionHandler);
        // 3、把请求对象放入缓存队列 unsent 里
        unsent.put(node, clientRequest);

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        // 4、唤醒处在阻塞过程中的 selector.poll() 方法，这样能对发送的请求发起网络请求。
        client.wakeup();
        return completionHandler.future;
    }

    public Node leastLoadedNode() {
        lock.lock();
        try {
            return client.leastLoadedNode(time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    public boolean hasReadyNodes(long now) {
        lock.lock();
        try {
            return client.hasReadyNodes(now);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block waiting on the metadata refresh with a timeout.
     *
     * @return true if update succeeded, false otherwise.
     */
    public boolean awaitMetadataUpdate(Timer timer) {
        int version = this.metadata.requestUpdate();
        do {
            poll(timer);
        } while (this.metadata.updateVersion() == version && timer.notExpired());
        return this.metadata.updateVersion() > version;
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block
     * until it has completed).
     */
    boolean ensureFreshMetadata(Timer timer) {
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(timer.currentTimeMs()) == 0) {
            return awaitMetadataUpdate(timer);
        } else {
            // the metadata is already fresh
            return true;
        }
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either
     * on the current poll if one is active, or the next poll.
     */
    public void wakeup() {
        // wakeup should be safe without holding the client lock since it simply delegates to
        // Selector's wakeup, which is thread-safe
        log.debug("Received user wakeup");
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * Block indefinitely until the given request future has finished.
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(RequestFuture<?> future) {
        while (!future.isDone())
            poll(time.timer(Long.MAX_VALUE), future);
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     * @param future The request future to wait for
     * @param timer Timer bounding how long this method can block
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public boolean poll(RequestFuture<?> future, Timer timer) {
        do {
            // ConsumerNetworkClient 会不断调用底层组件 NetworkClient
            // 把 FindCoordinatorRequest 请求发送出去，直到发送成功或超时。
            poll(timer, future);
        } while (!future.isDone() && timer.notExpired());
        return future.isDone();
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(Timer timer) {
        poll(timer, null);
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     */
    public void poll(Timer timer, PollCondition pollCondition) {
        poll(timer, pollCondition, false);
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     * @param disableWakeup If TRUE disable triggering wake-ups
     */
    // 轮询所有可以发送的请求，真正的发送。此时获取到每一个请求都绑定 SelectorKey.OP_WRITE 事件
    public void poll(Timer timer, PollCondition pollCondition, boolean disableWakeup) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        firePendingCompletedRequests();

        lock.lock();
        try {
            // Handle async disconnects prior to attempting any sends
            handlePendingDisconnects();

            // send all the requests we can send now
            // 发送我们现在可以发送的所有请求
            long pollDelayMs = trySend(timer.currentTimeMs());

            // check whether the poll is still needed by the caller. Note that if the expected completion
            // condition becomes satisfied after the call to shouldBlock() (because of a fired completion
            // handler), the client will be woken up.

            // 调用 NetworkClient#poll() 方法监听底层网络连接，并处理网络数据读写
            if (pendingCompletion.isEmpty() && (pollCondition == null || pollCondition.shouldBlock())) {
                // if there are no requests in flight, do not block longer than the retry backoff
                long pollTimeout = Math.min(timer.remainingMs(), pollDelayMs);
                if (client.inFlightRequestCount() == 0)
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);
                client.poll(pollTimeout, timer.currentTimeMs());
            } else {
                client.poll(0, timer.currentTimeMs());
            }
            timer.update();

            // handle any disconnects by failing the active requests. note that disconnects must
            // be checked immediately following poll since any subsequent call to client.ready()
            // will reset the disconnect status

            // 处理断开连接的 node 的消息
            checkDisconnects(timer.currentTimeMs());
            if (!disableWakeup) {
                // trigger wakeups after checking for disconnects so that the callbacks will be ready
                // to be fired on the next call to poll()
                // 如果有 selector.poll() 阻塞中断请求而且有方法标记不能中断，则抛出异常
                maybeTriggerWakeup();
            }
            // throw InterruptException if this thread is interrupted
            maybeThrowInterruptException();

            // try again to send requests since buffer space may have been
            // cleared or a connect finished in the poll
            // 再次发送我们现在可以发送的所有请求
            // 上面调用了 poll() 方法，send 就有可能发送成功了，也可能增加了网络连接，所以再次从 unsent 集合中取请求做预发送
            trySend(timer.currentTimeMs());

            // fail requests that couldn't be sent if they have expired
            // 处理 unsent 中超时的请求
            failExpiredRequests(timer.currentTimeMs());

            // clean unsent requests collection to keep the map from growing indefinitely
            unsent.clean();
        } finally {
            lock.unlock();
        }

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        // 调用 ConsumerNetworkClient#firePendingCompletedRequests() 方法回调上层请求的回调处理器
        firePendingCompletedRequests();

        metadata.maybeThrowAnyException();
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups.
     */
    public void pollNoWakeup() {
        poll(time.timer(0), null, true);
    }

    /**
     * Poll for network IO in best-effort only trying to transmit the ready-to-send request
     * Do not check any pending requests or metadata errors so that no exception should ever
     * be thrown, also no wakeups be triggered and no interrupted exception either.
     */
    public void transmitSends() {
        Timer timer = time.timer(0);

        // do not try to handle any disconnects, prev request failures, metadata exception etc;
        // just try once and return immediately
        lock.lock();
        try {
            // send all the requests we can send now
            trySend(timer.currentTimeMs());

            client.poll(0, timer.currentTimeMs());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block until all pending requests from the given node have finished.
     * @param node The node to await requests from
     * @param timer Timer bounding how long this method can block
     * @return true If all requests finished, false if the timeout expired first
     */
    public boolean awaitPendingRequests(Node node, Timer timer) {
        while (hasPendingRequests(node) && timer.notExpired()) {
            poll(timer);
        }
        return !hasPendingRequests(node);
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        lock.lock();
        try {
            return unsent.requestCount(node) + client.inFlightRequestCount(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests(Node node) {
        if (unsent.hasRequests(node))
            return true;
        lock.lock();
        try {
            return client.hasInFlightRequests(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        lock.lock();
        try {
            return unsent.requestCount() + client.inFlightRequestCount();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests() {
        if (unsent.hasRequests())
            return true;
        lock.lock();
        try {
            return client.hasInFlightRequests();
        } finally {
            lock.unlock();
        }
    }

    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null)
                break;

            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }

        // wakeup the client in case it is blocking in poll for this future's completion
        if (completedRequestsFired)
            client.wakeup();
    }

    private void checkDisconnects(long now) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        for (Node node : unsent.nodes()) {
            // 检测消费者与每个 Node 之间的连接状态
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                // 在调用请求回调之前删除条目以避免回调处理再次遍历未发送列表的协调器故障
                Collection<ClientRequest> requests = unsent.remove(node);
                for (ClientRequest request : requests) {
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    AuthenticationException authenticationException = client.authenticationException(node);
                    // 调用 ClientRequest 的回调函数
                    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
                            request.callback(), request.destination(), request.createdTimeMs(), now, true,
                            null, authenticationException, null));
                }
            }
        }
    }

    private void handlePendingDisconnects() {
        lock.lock();
        try {
            while (true) {
                Node node = pendingDisconnects.poll();
                if (node == null)
                    break;

                failUnsentRequests(node, DisconnectException.INSTANCE);
                client.disconnect(node.idString());
            }
        } finally {
            lock.unlock();
        }
    }

    public void disconnectAsync(Node node) {
        pendingDisconnects.offer(node);
        client.wakeup();
    }

    private void failExpiredRequests(long now) {
        // clear all expired unsent requests and fail their corresponding futures
        // 清除所有过期的未发送请求并使其相应的 futures 失败
        Collection<ClientRequest> expiredRequests = unsent.removeExpiredRequests(now);
        for (ClientRequest request : expiredRequests) {
            RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
            // 调用回调函数
            handler.onFailure(new TimeoutException("Failed to send request after " + request.requestTimeoutMs() + " ms."));
        }
    }

    private void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        lock.lock();
        try {
            Collection<ClientRequest> unsentRequests = unsent.remove(node);
            for (ClientRequest unsentRequest : unsentRequests) {
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                handler.onFailure(e);
            }
        } finally {
            lock.unlock();
        }
    }

    // Visible for testing
    long trySend(long now) {
        long pollDelayMs = maxPollTimeoutMs;

        // send any requests that can be sent now
        // 遍历 Broker 节点
        for (Node node : unsent.nodes()) {
            // 获取该节点的所有待发送的请求，存储在 unsent 队列中的请求
            Iterator<ClientRequest> iterator = unsent.requestIterator(node);
            if (iterator.hasNext())
                // 计算最小过期时间
                pollDelayMs = Math.min(pollDelayMs, client.pollDelayMs(node, now));

            // 轮询队列里的请求，预发送请求。
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                // 当客户端准备好后
                // 调用 NetworkClient#ready() 确保与目标节点建立了连接
                if (client.ready(node, now)) {
                    // 这里的 client 表示 NetworkClient，其中涉及到 Kafka 网络通讯的内容，
                    // 内部绑定 SelectionKey.OP_WRITE 事件
                    client.send(request, now);
                    // 删除队列中对应的请求
                    iterator.remove();
                } else {
                    // try next node when current node is not ready
                    break;
                }
            }
        }
        // 返回 poll 延迟时间
        return pollDelayMs;
    }

    public void maybeTriggerWakeup() {
        // 通过 wakeupDisabled 检测是否在执行不可中断的方法，通过 wakeup 检测是否有中断请求。
        if (!wakeupDisabled.get() && wakeup.get()) {
            log.debug("Raising WakeupException in response to user wakeup");
            // 重置中断标志
            wakeup.set(false);
            throw new WakeupException();
        }
    }

    private void maybeThrowInterruptException() {
        if (Thread.interrupted()) {
            throw new InterruptException(new InterruptedException());
        }
    }

    public void disableWakeups() {
        wakeupDisabled.set(true);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            client.close();
        } finally {
            lock.unlock();
        }
    }


    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     */
    public boolean isUnavailable(Node node) {
        lock.lock();
        try {
            return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check for an authentication error on a given node and raise the exception if there is one.
     */
    public void maybeThrowAuthFailure(Node node) {
        lock.lock();
        try {
            AuthenticationException exception = client.authenticationException(node);
            if (exception != null)
                throw exception;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, AbstractRequest.Builder)}
     * should be used.
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        lock.lock();
        try {
            client.ready(node, time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    private class RequestFutureCompletionHandler implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;
        private ClientResponse response;
        private RuntimeException e;

        private RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            } else if (response.authenticationException() != null) {
                future.raise(response.authenticationException());
            } else if (response.wasDisconnected()) {
                log.debug("Cancelled request with header {} due to node {} being disconnected",
                        response.requestHeader(), response.destination());
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                future.complete(response);
            }
        }

        // 响应异常时的处理
        public void onFailure(RuntimeException e) {
            //获取异常并把回调对象放到 pendingCompletion 集合里
            this.e = e;
            pendingCompletion.add(this);
        }

        // 响应成功时的处理
        @Override
        public void onComplete(ClientResponse response) {
            // 获取 response 并把回调对象放到 pendingCompletion 集合里
            this.response = response;
            pendingCompletion.add(this);
        }
    }

    /**
     * When invoking poll from a multi-threaded environment, it is possible that the condition that
     * the caller is awaiting has already been satisfied prior to the invocation of poll. We therefore
     * introduce this interface to push the condition checking as close as possible to the invocation
     * of poll. In particular, the check will be done while holding the lock used to protect concurrent
     * access to {@link org.apache.kafka.clients.NetworkClient}, which means implementations must be
     * very careful about locking order if the callback must acquire additional locks.
     */
    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }

    /*
     * A thread-safe helper class to hold requests per node that have not been sent yet
     */
    private final static class UnsentRequests {
        private final ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> unsent;

        private UnsentRequests() {
            unsent = new ConcurrentHashMap<>();
        }

        public void put(Node node, ClientRequest request) {
            // the lock protects the put from a concurrent removal of the queue for the node
            // 首先使用 synchronized 加锁，加锁对象 unsent
            synchronized (unsent) {
                // 根据 node 取出要发送请求的队列，如果队列不为空就取出队列，如果队列是空的就创建一个队列并放入 unsent 中
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
                if (requests == null) {
                    requests = new ConcurrentLinkedQueue<>();
                    unsent.put(node, requests);
                }
                // 把请求放入队列中。
                requests.add(request);
            }
        }

        public int requestCount(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? 0 : requests.size();
        }

        public int requestCount() {
            int total = 0;
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                total += requests.size();
            return total;
        }

        public boolean hasRequests(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests != null && !requests.isEmpty();
        }

        public boolean hasRequests() {
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                if (!requests.isEmpty())
                    return true;
            return false;
        }

        private Collection<ClientRequest> removeExpiredRequests(long now) {
            // 过期请求集合
            List<ClientRequest> expiredRequests = new ArrayList<>();
            // 1、遍历请求
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) {
                Iterator<ClientRequest> requestIterator = requests.iterator();
                while (requestIterator.hasNext()) {
                    ClientRequest request = requestIterator.next();
                    // 2、计算请求在缓冲区存在的时间
                    long elapsedMs = Math.max(0, now - request.createdTimeMs());
                    // 3、如果请求在缓冲区存在的时间大于超时时间就把请求加入过期集合，并从 unsent 集合中删除。
                    if (elapsedMs > request.requestTimeoutMs()) {
                        expiredRequests.add(request);
                        requestIterator.remove();
                    } else
                        break;
                }
            }
            return expiredRequests;
        }

        public void clean() {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                Iterator<ConcurrentLinkedQueue<ClientRequest>> iterator = unsent.values().iterator();
                while (iterator.hasNext()) {
                    ConcurrentLinkedQueue<ClientRequest> requests = iterator.next();
                    if (requests.isEmpty())
                        iterator.remove();
                }
            }
        }

        public Collection<ClientRequest> remove(Node node) {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                // 从缓冲区移除请求
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.remove(node);
                // 返回请求列表
                return requests == null ? Collections.<ClientRequest>emptyList() : requests;
            }
        }

        public Iterator<ClientRequest> requestIterator(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? Collections.<ClientRequest>emptyIterator() : requests.iterator();
        }

        public Collection<Node> nodes() {
            return unsent.keySet();
        }
    }

}
