/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  // 将给定的定时任务插入到时间轮上，等待后续延迟执行
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  //  向前推进时钟，执行已达过期时间的延迟任务
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  // 获取时间轮上总的定时任务数
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  // 关闭定时器
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // timeout timer
  // 单线程的线程池用于异步执行定时任务
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  // 延迟队列保存所有 Bucket，即所有 TimerTaskList 对象
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 总定时任务数
  private[this] val taskCounter = new AtomicInteger(0)
  // 时间轮对象
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  // 线程安全的读写锁
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    // 首先获取读锁，在没有线程持有写锁的前提下，多个线程能够同时向时间轮添加定时任务
    readLock.lock()
    try {
      // 调用 addTimerTaskEntry 执行插入逻辑
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      // 释放读锁
      readLock.unlock()
    }
  }

  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 首先判断是否添加成功
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 如果添加失败，则判断定时任务是否已取消，如果没有则说明定时任务已过期否则添加肯定成功的
      if (!timerTaskEntry.cancelled)
      // 此时将定时任务添加到处理延迟任务的线程池中
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    // 从延迟队列 delayQueue 中取出下一个已过期的 Bucket
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    // 如果不为空
    if (bucket != null) {
      // 获取写锁，一旦有线程持有写锁，其他任何线程执行 add 或 advanceClock 方法时会阻塞
      writeLock.lock()
      try {
        // 如果 bucket 不为空，则一直重复执行
        while (bucket != null) {
          // 推动时间轮向前推进到 Bucket 的过期时间点，这里只需要修改 TimingWheel#currentTime 即可。
          timingWheel.advanceClock(bucket.getExpiration)
          // 将该 Bucket 下的所有定时任务重写回到时间轮
          bucket.flush(reinsert)
          // 读取下一个 Bucket 对象
          bucket = delayQueue.poll()
        }
      } finally {
        // 释放写锁
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}

