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
package org.apache.kafka.common.network;

import java.nio.ByteBuffer;

/**
 * A size delimited Send that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkSend extends ByteBufferSend {

    // 实例化
    public NetworkSend(String destination, ByteBuffer buffer) {
        // 调用父类的方法初始化
        super(destination, sizeBuffer(buffer.remaining()), buffer);
    }

    // 用来构造4个字节的 sizeBuffer
    private static ByteBuffer sizeBuffer(int size) {
        // 先分配一个4个字节的ByteBuffer
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        // 写入size长度值
        sizeBuffer.putInt(size);
        // 重置 position
        sizeBuffer.rewind();
        // 返回 sizeBuffer
        return sizeBuffer;
    }

}
