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

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;

/**
 * A request being sent to the server. This holds both the network send as well as the client-level metadata.
 */
// ClientRequest 是客户端的请求，封装了 requestBuilder
public final class ClientRequest {

    // 节点地址
    private final String destination;
    // ClientRequest 中通过 requestBuilder 给不同类型的请求设置不同的请求内容
    private final AbstractRequest.Builder<?> requestBuilder;
    // 请求头的 correlationId
    private final int correlationId;
    // 请求头的 clientid
    private final String clientId;
    // 创建时间
    private final long createdTimeMs;
    // 是否需要进行响应
    private final boolean expectResponse;
    // 请求的超时时间
    private final int requestTimeoutMs;
    // 回调函数 用来处理响应
    private final RequestCompletionHandler callback;

    /**
     * @param destination The brokerId to send the request to
     * @param requestBuilder The builder for the request to make
     * @param correlationId The correlation id for this client request
     * @param clientId The client ID to use for the header
     * @param createdTimeMs The unix timestamp in milliseconds for the time at which this request was created.
     * @param expectResponse Should we expect a response message or is this request complete once it is sent?
     * @param callback A callback to execute when the response has been received (or null if no callback is necessary)
     */
    public ClientRequest(String destination,
                         AbstractRequest.Builder<?> requestBuilder,
                         int correlationId,
                         String clientId,
                         long createdTimeMs,
                         boolean expectResponse,
                         int requestTimeoutMs,
                         RequestCompletionHandler callback) {
        this.destination = destination;
        this.requestBuilder = requestBuilder;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.createdTimeMs = createdTimeMs;
        this.expectResponse = expectResponse;
        this.requestTimeoutMs = requestTimeoutMs;
        this.callback = callback;
    }

    @Override
    public String toString() {
        return "ClientRequest(expectResponse=" + expectResponse +
            ", callback=" + callback +
            ", destination=" + destination +
            ", correlationId=" + correlationId +
            ", clientId=" + clientId +
            ", createdTimeMs=" + createdTimeMs +
            ", requestBuilder=" + requestBuilder +
            ")";
    }

    public boolean expectResponse() {
        return expectResponse;
    }

    public ApiKeys apiKey() {
        return requestBuilder.apiKey();
    }

    public RequestHeader makeHeader(short version) {
        short requestApiKey = requestBuilder.apiKey().id;
        return new RequestHeader(
            new RequestHeaderData().
                setRequestApiKey(requestApiKey).
                setRequestApiVersion(version).
                setClientId(clientId).
                setCorrelationId(correlationId),
            ApiKeys.forId(requestApiKey).requestHeaderVersion(version));
    }

    public AbstractRequest.Builder<?> requestBuilder() {
        return requestBuilder;
    }

    public String destination() {
        return destination;
    }

    public RequestCompletionHandler callback() {
        return callback;
    }

    public long createdTimeMs() {
        return createdTimeMs;
    }

    public int correlationId() {
        return correlationId;
    }

    public int requestTimeoutMs() {
        return requestTimeoutMs;
    }
}
