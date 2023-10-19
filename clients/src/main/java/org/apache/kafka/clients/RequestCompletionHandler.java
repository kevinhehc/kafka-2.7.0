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

/**
 * A callback interface for attaching an action to be executed when a request is complete and the corresponding response
 * has been received. This handler will also be invoked if there is a disconnection while handling the request.
 */
public interface RequestCompletionHandler {

    // 接口 RequestCompletionHandler 就一个方法 onComplete()，
    // 上面 callback 对 onComplete() 的实现主要是用返回值 response 当参数调用 handleProduceResponse()方法。
    // 消息发送的时候并不会把 callback 方法发送到 Broker 端，
    // 而是把 callback 放在 NetworkClient 里等待响应回来后根据响应去调用 callback。
    public void onComplete(ClientResponse response);

}
