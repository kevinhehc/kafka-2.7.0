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

import org.apache.kafka.common.memory.MemoryPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;
    private static final Logger log = LoggerFactory.getLogger(NetworkReceive.class);
    // 空 ByteBuffer
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    // final类型，用来确定对应 channel id。
    private final String source;
    // final类型，存储响应消息数据长度，大小为4字节。
    private final ByteBuffer size;
    // 响应消息数据的最大长度
    private final int maxSize;
    // ByteBuffer 内存池
    private final MemoryPool memoryPool;
    // 已读取字节大小
    private int requestedBufferSize = -1;
    // 存储响应消息数据体
    private ByteBuffer buffer;


    // 初始化构造函数
    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(String source) {
        this.source = source;
        // 分配4个字节大小的数据长度
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        // 能接收消息的最大长度
        this.maxSize = UNLIMITED;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = MemoryPool.NONE;
    }

    public NetworkReceive(int maxSize, String source, MemoryPool memoryPool) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
        this.memoryPool = memoryPool;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        // 响应消息头已读完 && 响应消息体已读完
        return !size.hasRemaining() && buffer != null && !buffer.hasRemaining();
    }

    public long readFrom(ScatteringByteChannel channel) throws IOException {
        // 读取数据总大小
        int read = 0;
        // 1.判断响应消息数据长度的 ByteBuffer 是否读完
        if (size.hasRemaining()) {
            // 2.还有剩余，直接读取消息数据的长度
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();

            // 3.每次读取后，累加到总读取数据大小里
            read += bytesRead;
            // 4.判断响应消息数据长度的缓存是否读完了
            if (!size.hasRemaining()) {
                // 5.重置position
                size.rewind();
                // 6.读取响应消息数据长度
                int receiveSize = size.getInt();
                // 7.如果有异常就抛出
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");
                // 8.将读到数据长度赋值已读取字节大小，即数据体的大小
                requestedBufferSize = receiveSize; //may be 0 for some payloads (SASL)
                if (receiveSize == 0) {
                    buffer = EMPTY_BUFFER;
                }
            }
        }
        // 9.如果数据体buffer还没有分配，且响应消息数据头已读完
        if (buffer == null && requestedBufferSize != -1) { //we know the size we want but havent been able to allocate it yet
            // 10.分配requestedBufferSize字节大小的内存空间给数据体buffer
            buffer = memoryPool.tryAllocate(requestedBufferSize);
            if (buffer == null)
                log.trace("Broker low on memory - could not allocate buffer of size {} for source {}", requestedBufferSize, source);
        }
        // 11.判断buffer是否分配成功
        if (buffer != null) {
            // 12.把channel里的数据读到buffer中
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            // 13.累计读取数据总大小
            read += bytesRead;
        }

        // 14. 返回总大小
        return read;
    }

    @Override
    public boolean requiredMemoryAmountKnown() {
        return requestedBufferSize != -1;
    }

    @Override
    public boolean memoryAllocated() {
        return buffer != null;
    }


    @Override
    public void close() throws IOException {
        if (buffer != null && buffer != EMPTY_BUFFER) {
            memoryPool.release(buffer);
            buffer = null;
        }
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

    public int bytesRead() {
        if (buffer == null)
            return size.position();
        return buffer.position() + size.position();
    }

    /**
     * Returns the total size of the receive including payload and size buffer
     * for use in metrics. This is consistent with {@link NetworkSend#size()}
     */
    // 返回大小
    public int size() {
        return payload().limit() + size.limit();
    }

}
