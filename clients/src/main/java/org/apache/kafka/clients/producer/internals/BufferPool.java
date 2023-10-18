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
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public class BufferPool {

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    // 整个BufferPool总内存大小 默认32M
    private final long totalMemory;
    // 当前BufferPool管理的单个ByteBuffer大小，16k
    private final int poolableSize;
    // 因为有多线程并发分配和回收ByteBuffer，用锁控制并发，保证线程安全。
    private final ReentrantLock lock;
    // 对应一个ArrayDeque<ByteBuffer> 队列，其中缓存了固定大小的 ByteBuffer 对象
    private final Deque<ByteBuffer> free;
    // 此队列记录因申请不到足够空间而阻塞的线程对应的Condition 对象
    private final Deque<Condition> waiters;
    /** Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize.  */
    // 非池化可用的内存即totalMemory减去free列表中的全部ByteBuffer的大小
    private long nonPooledAvailableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;
    private boolean closed;

    /**
     * Create a new buffer pool
     *
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    // 构造函数
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        // 总的内存
        this.totalMemory = memory;
        // 默认的池外内存，就是总的内存
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        MetricName totalMetricName = metrics.metricName("bufferpool-wait-time-total",
                                                   metricGrpName,
                                                   "The total time an appender waits for space allocation.");

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        MetricName bufferExhaustedRateMetricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName bufferExhaustedTotalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(bufferExhaustedRateMetricName, bufferExhaustedTotalMetricName));

        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName));
        this.closed = false;
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    // 分配指定空间的缓存，如果缓冲区中没有足够的空闲空间，那么会阻塞线程，直到超时或得到足够空间
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        // 1.判断申请的内存是否大于总内存
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        // 初始化buffer
        ByteBuffer buffer = null;
        // 2.加锁，保证线程安全。
        this.lock.lock();

        // 如果当前BufferPool处于关闭状态，则直接抛出异常
        if (this.closed) {
            this.lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }

        try {
            // check if we have a free buffer of the right size pooled
            // 3.申请内存大小恰好为16k 且free缓存池不为空
            if (size == poolableSize && !this.free.isEmpty())
                // 从free队列取出一个ByteBuffer
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            // 对于申请内存大小非16k情况
            // 先计算free缓存池总空间大小，判断是否足够
            int freeListSize = freeSize() * this.poolableSize;
            // 4.当前BufferPool能够释放出申请内存大小的空间
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                // 5.如果size大于非池化可用内存大小，就循环从free缓存池里释放出来空闲Bytebuffer补充到nonPooledAvailableMemory中，
                // 直到满足size大小为止。
                freeUp(size);
                // 释放非池化可用内存大小
                this.nonPooledAvailableMemory -= size;
            } else {
                // we are out of memory and will have to block
                // 如果当前BufferPool不够提供申请内存大小，则需要阻塞当前线程
                // 累计已经释放的内存
                int accumulated = 0;
                // 创建对应的Condition，阻塞自己等待别的线程释放内存
                Condition moreMemory = this.lock.newCondition();
                try {
                    // 计算当前线程最大阻塞时长
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    // 把自己添加到等待队列中末尾，保持公平性，先来的先获取内存，防止饥饿
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    // 循环等待直到分配成功或超时
                    while (accumulated < size) {
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            // 当前线程阻塞等待，返回结果为false则表示阻塞超时
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            recordWaitTime(timeNs);
                        }

                        if (this.closed)
                            throw new KafkaException("Producer closed while allocating memory");

                        if (waitingTimeElapsed) {
                            this.metrics.sensor("buffer-exhausted-records").record();
                            throw new BufferExhaustedException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        // 申请内存大小是16k，且free缓存池有了空闲的ByteBuffer
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            // 从free队列取出一个ByteBuffer
                            buffer = this.free.pollFirst();
                            // 计算累加器
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
                            // 释放空间给非池化可用内存，并继续等待空闲空间，如果分配多了只取够size的空间
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            // 释放非池化可用内存大小
                            this.nonPooledAvailableMemory -= got;
                            // 累计分配了多少空间
                            accumulated += got;
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    // 如果循环有异常，将已释放的空间归还给非池化可用内存
                    this.nonPooledAvailableMemory += accumulated;
                    //把自己从等待队列中移除并结束
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            // 当非池化可用内存有内存或free缓存池有空闲ByteBufer且等待队列里有线程正在等待
            try {
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    // 唤醒队列里正在等待的线程
                    this.waiters.peekFirst().signal();
            } finally {
                // Another finally... otherwise find bugs complains

                // 解锁
                lock.unlock();
            }
        }

        // 说明空间足够，并且有足够空闲的了。可以执行真正的分配空间了。
        if (buffer == null)
            // 没有正好的buffer，从缓冲区外(JVM Heap)中直接分配内存
            return safeAllocateByteBuffer(size);
        else
            // 直接复用free缓存池的ByteBuffer
            return buffer;
    }

    // Protected for testing
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            //分配空间
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            //返回buffer
            return buffer;
        } finally {
            if (error) {
                //分配失败了, 加锁，操作内存pool
                this.lock.lock();
                try {
                    //归还空间给非池化可用内存
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty())
                        //有其他在等待的线程的话，唤醒其他线程
                        this.waiters.peekFirst().signal();
                } finally {
                    // 加锁不忘解锁
                    this.lock.unlock();
                }
            }
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        // 从JVM Heap中分配空间
        return ByteBuffer.allocate(size);
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    // 不断从free队列中释放空闲的ByteBuffer来补充非池化可用内存
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        // 1.加锁，保证线程安全。
        lock.lock();
        try {
            // 2.如果待释放的size大小为16k，则直接放入free队列中
            if (size == this.poolableSize && size == buffer.capacity()) {
                // 清空buffer
                buffer.clear();
                // 释放buffer到free队列里
                this.free.add(buffer);
            } else {
                //如果非16k，则由JVM GC来回收ByteBuffer并增加非池化可用内存
                this.nonPooledAvailableMemory += size;
            }
            // 3.唤醒waiters中的第一个阻塞线程
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }

    /**
     * Closes the buffer pool. Memory will be prevented from being allocated, but may be deallocated. All allocations
     * awaiting available memory will be notified to abort.
     */
    public void close() {
        this.lock.lock();
        this.closed = true;
        try {
            for (Condition waiter : this.waiters)
                waiter.signal();
        } finally {
            this.lock.unlock();
        }
    }
}
