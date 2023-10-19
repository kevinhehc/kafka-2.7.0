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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 */
// 用来缓存已经发送出去或者正在发送但均还没有收到响应的  ClientRequest 请求集合
final class InFlightRequests {

    // 每个连接最大执行中的请求数
    private final int maxInFlightRequestsPerConnection;
    // 节点 Node 至客户端请求双端队列 Deque<NetworkClient.InFlightRequest> 的映射集合，key为 NodeId, value 是请求队列
    private final Map<String, Deque<NetworkClient.InFlightRequest>> requests = new HashMap<>();
    /** Thread safe total number of in flight requests. */
    // 线程安全的 inFlightRequestCount
    private final AtomicInteger inFlightRequestCount = new AtomicInteger(0);

    // 设置每个连接最大执行中的请求数
    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * Add the given request to the queue for the connection it was directed to
     */
    // 将请求添加到队列首部
    public void add(NetworkClient.InFlightRequest request) {
        // 这个请求要发送到哪个 Broker 节点上
        String destination = request.destination;
        // 从 requests 集合中根据给定请求的目标 Node 节点获取对应 Deque<ClientRequest> 双端队列 reqs
        Deque<NetworkClient.InFlightRequest> reqs = this.requests.get(destination);
        // 如果双端队列reqs为null
        if (reqs == null) {
            // 构造一个双端队列 ArrayDeque 类型的 reqs
            reqs = new ArrayDeque<>();
            // 将请求目标 Node 节点至 reqs 的映射关系添加到 requests 集合
            this.requests.put(destination, reqs);
        }
        // 将请求 request 添加到 reqs 队首
        reqs.addFirst(request);
        // 增加计数
        inFlightRequestCount.incrementAndGet();
    }

    /**
     * Get the request queue for the given node
     */
    private Deque<NetworkClient.InFlightRequest> requestQueue(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty())
            throw new IllegalStateException("There are no in-flight requests for node " + node);
        return reqs;
    }

    /**
     * Get the oldest request (the one that will be completed next) for the given node
     */
    //  取出该连接对应的队列中最老的请求
    public NetworkClient.InFlightRequest completeNext(String node) {
        // 根据给定 Node 节点获取客户端请求双端队列 reqs，并从队尾出队
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollLast();
        // 递减计数器
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     * @param node The node id
     */
    public NetworkClient.InFlightRequest lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    /**
     * Complete the last request that was sent to a particular node.
     * @param node The node the request was sent to
     * @return The request
     */
    // 取出该连接对应的队列中最新的请求
    public NetworkClient.InFlightRequest completeLastSent(String node) {
        // 根据给定 Node 节点获取客户端请求双端队列 reqs，并从队首出队
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollFirst();
        // 递减计数器
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * Can we send more requests to this node?
     *
     * @param node Node in question
     * @return true iff we have no requests still being sent to the given node
     */
    //  判断该连接是否还能发送请求
    public boolean canSendMore(String node) {
        // 获取节点对应的双端队列
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        // 判断条件 队列为空 || (队首已经发送完成 && 队列中没有堆积更多的请求)
        return queue == null || queue.isEmpty() ||
               (queue.peekFirst().send.completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    /**
     * Return the number of in-flight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    public int count(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Return true if there is no in-flight request directed at the given node and false otherwise
     */
    public boolean isEmpty(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty();
    }

    /**
     * Count all in-flight requests for all nodes. This method is thread safe, but may lag the actual count.
     */
    public int count() {
        return inFlightRequestCount.get();
    }

    /**
     * Return true if there is no in-flight request and false otherwise
     */
    public boolean isEmpty() {
        for (Deque<NetworkClient.InFlightRequest> deque : this.requests.values()) {
            if (!deque.isEmpty())
                return false;
        }
        return true;
    }

    /**
     * Clear out all the in-flight requests for the given node and return them
     *
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    public Iterable<NetworkClient.InFlightRequest> clearAll(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null) {
            return Collections.emptyList();
        } else {
            final Deque<NetworkClient.InFlightRequest> clearedRequests = requests.remove(node);
            inFlightRequestCount.getAndAdd(-clearedRequests.size());
            return () -> clearedRequests.descendingIterator();
        }
    }

    // 判断是否超时
    private Boolean hasExpiredRequest(long now, Deque<NetworkClient.InFlightRequest> deque) {
        for (NetworkClient.InFlightRequest request : deque) {
            long timeSinceSend = Math.max(0, now - request.sendTimeMs);
            // 节点队列请求是否超时，如果有一个请求超时，就认为这个 broker 超时了，默认 30000 ms。
            if (timeSinceSend > request.requestTimeoutMs)
                return true;
        }
        return false;
    }

    /**
     * Returns a list of nodes with pending in-flight request, that need to be timed out
     *
     * @param now current time in milliseconds
     * @return list of nodes
     */
    // 收集超时请求的节点集合
    public List<String> nodesWithTimedOutRequests(long now) {
        List<String> nodeIds = new ArrayList<>();
        // 遍历节点队列
        for (Map.Entry<String, Deque<NetworkClient.InFlightRequest>> requestEntry : requests.entrySet()) {
            // 获取节点id
            String nodeId = requestEntry.getKey();
            // 获取队列数据
            Deque<NetworkClient.InFlightRequest> deque = requestEntry.getValue();
            // 判断是否超时
            if (hasExpiredRequest(now, deque))
                // 超时后将节点加入
                nodeIds.add(nodeId);
        }
        return nodeIds;
    }

}
