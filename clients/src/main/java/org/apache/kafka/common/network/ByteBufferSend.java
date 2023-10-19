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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * A send backed by an array of byte buffers
 */
public class ByteBufferSend implements Send {

    // 要把数据写入目标的 channel id
    private final String destination;
    // 总共要写多少字节数据
    private final int size;
    // 用于写入channel里的ByteBuffer数组，说明kafka一次最大传输字节是有限定的
    protected final ByteBuffer[] buffers;
    // 总共还剩多少字节没有写完
    private int remaining;
    private boolean pending = false;

    public ByteBufferSend(String destination, ByteBuffer... buffers) {
        this.destination = destination;
        this.buffers = buffers;
        for (ByteBuffer buffer : buffers)
            remaining += buffer.remaining();
        // 计算需要写入字节的总和
        this.size = remaining;
    }

    @Override
    public String destination() {
        // 返回对应的channel id
        return destination;
    }

    @Override
    public boolean completed() {
        // 判断是否完成 即没有剩余&pending=false
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    // 将字节流数据写入到channel中
    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        // 1.调用nio底层write方法把buffers写入传输层返回写入的字节数
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        // 2.计算还剩多少字节没有写入传输层
        remaining -= written;
        // 每次发送 都检查是否
        pending = TransportLayers.hasPendingWrites(channel);
        return written;
    }

    public long remaining() {
        return remaining;
    }

    @Override
    public String toString() {
        return "ByteBufferSend(" +
            "destination='" + destination + "'" +
            ", size=" + size +
            ", remaining=" + remaining +
            ", pending=" + pending +
            ')';
    }
}
