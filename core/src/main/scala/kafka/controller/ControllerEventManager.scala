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

package kafka.controller

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

object ControllerEventManager {
  // 线程名称
  val ControllerEventThreadName = "controller-event-thread"
  // 监控指标
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

trait ControllerEventProcessor {
  // 接收一个 Controller 事件，并进行处理
  def process(event: ControllerEvent): Unit
  // 接收一个 Controller 事件，并抢占队列之前的事件进行优先处理。
  def preempt(event: ControllerEvent): Unit
}

class QueuedEvent(val event: ControllerEvent, // ControllerEvent类，表示 Controller 事件
                  val enqueueTimeMs: Long) { // 表示 Controller 事件被放入到事件队列的时间戳
  // 标识事件是否开始被处理
  val processingStarted = new CountDownLatch(1)
  // 标识事件是否被处理过
  val spent = new AtomicBoolean(false)

  // 处理事件
  def process(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processingStarted.countDown()
    processor.process(event)
  }

  // 抢占式处理事件
  def preempt(processor: ControllerEventProcessor): Unit = {
    // 若已经被处理过，直接返回
    if (spent.getAndSet(true))
      return
    // 调用 ControllerEventProcessor 的 preempt 方法处理抢占式事件
    processor.preempt(event)
  }

  // 阻塞等待事件被处理完成
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}


// 用于管理控制器事件
class ControllerEventManager(controllerId: Int, // 控制器的 ID
                             processor: ControllerEventProcessor, // 用于处理控制器事件的 ControllerEventProcessor 实例
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer], // 以控制器状态和 Kafka 计时器为键值对的映射，用于度量控制器事件的速率和时间
                             eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup { // 事件队列等待超时的时间，默认为 300000 毫秒（5分钟）
  import ControllerEventManager._

  // 一个私有的 volatile 变量，用于存储控制器的当前状态。通过 @volatile 标记，
  // 表示该变量可能在多个线程之间进行并发访问，且每次访问都从共享内存中读取最新值
  @volatile private var _state: ControllerState = ControllerState.Idle
  // 一个重入锁，用于保证在多线程环境下对队列的操作互斥进行
  private val putLock = new ReentrantLock()
  // 一个基于链表的阻塞队列，用于存储待处理的事件
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test
  // 一个 ControllerEventThread 实例，用于处理事件队列中的事件
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  // EventQueueSizeMetricName 是度量事件队列大小的指标名称。
  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  // EventQueueTimeMetricName 是事件队列等待时间的指标名称
  // newGauge(EventQueueSizeMetricName, () => queue.size) 是一个用于创建新度量指标的方法，用于度量事件队列的当前大小。
  // 该方法通过闭包将 queue.size 作为度量值，返回一个 Gauge 实例，用于度量事件队列大小。
  newGauge(EventQueueSizeMetricName, () => queue.size)

  // 对外提供了 state 方法，用于获取控制器的状态。
  def state: ControllerState = _state

  // start方法启动了控制器线程。
  def start(): Unit = thread.start()

  // 用于关闭控制器线程
  def close(): Unit = {
    try {
      // 停止控制器线程的消息消费，即将消息处理线程停掉
      thread.initiateShutdown()
      // 将 ShutdownEventThread 写入到阻塞事件队列中，表示控制器线程已经停掉，需要停止事件的消费
      clearAndPut(ShutdownEventThread)
      // 等待控制器线程的停止操作完成
      thread.awaitShutdown()
    } finally {
      // 删除控制器的度量信息，以便等待垃圾回收
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    // 构建QueuedEvent实例
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    // 写入到事件队列
    queue.put(queuedEvent)
    // 返回新建QueuedEvent实例
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock){
    // 优先处理抢占式事件,创建一个 ArrayList 集合 preemptedEvents，用于存储被抢占的事件
    val preemptedEvents = new ArrayList[QueuedEvent]()
    // 将队列中的所有事件都取出，并存储到 preemptedEvents 集合中
    queue.drainTo(preemptedEvents)
    // 针对 preemptedEvents 集合中的每个事件，调用 preempt 方法，执行被抢占的处理逻辑
    preemptedEvents.forEach(_.preempt(processor))
    // 将新的事件 event 放入队列中
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      // 从事件队列中获取待处理的Controller事件，否则等待
      val dequeued = pollFromEventQueue()
      dequeued.event match {
        // 如果是关闭线程事件，什么都不用做。关闭线程由外部来执行
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state

          // 更新对应事件在队列中保存的时间
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            // 定义一个事件处理函数，调用ControllerEventProcessor类的process()函数执行逻辑
            def process(): Unit = dequeued.process(processor)

            // 处理事件，同时计算处理速
            rateAndTimeMetrics.get(state) match {
              // 延迟调度
              case Some(timer) => timer.time { process() }
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  // 从事件队列中获取待处理的Controller事件
  private def pollFromEventQueue(): QueuedEvent = {
    // 获取事件队列中事件的数量，记录在count变量中
    val count = eventQueueTimeHist.count()
    if (count != 0) {
      // 使用指定的超时时间从队列中获取一个事件，记录在 event 变量中。
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      // 如果获取到的事件为 null，则清除事件队列时延的统计信息，并且从队列中取出一个事件
      if (event == null) {
        eventQueueTimeHist.clear()
        // 由于队列调用时take() 函数时阻塞的，直到队列中有事件才执行后续逻辑。
        queue.take()
      } else {
        // 如果获取到的事件非 null，则直接返回获取的事件
        event
      }
    } else {
      // 由于队列调用时take() 函数时阻塞的，直到队列中有事件才执行后续逻辑。
      queue.take()
    }
  }

}
