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

import java.util.concurrent.{Delayed, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Time

import scala.math._

@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  private[this] val root = new TimerTaskEntry(null, -1)
  root.next = root
  root.prev = root

  private[this] val expiration = new AtomicLong(-1L)

  // Set the bucket's expiration time
  // Returns true if the expiration time is changed
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time
  def getExpiration: Long = expiration.get

  // Apply the supplied function to each of tasks in this list
  // 对列表中的每个任务执行提供的函数 f
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next

        // 如果 entry 没有被取消，就对其对应的 timerTask 执行函数 f
        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    while (!done) {
      // Remove the timer task entry if it is already in any other list
      // We do this outside of the sync block below to avoid deadlocking.
      // We may retry until timerTaskEntry.list becomes null.
      // 检查 timerTaskEntry 是否已经在其他列表中，如果是，则先将其从其他列表中移除。
      // 将该操作放在同步块之外是为了避免死锁
      // 在 timerTaskEntry.list 变为 null 之前，我们可能会重试
      timerTaskEntry.remove()

      synchronized {
        // 在同步块的作用域内，使用 timerTaskEntry 锁定 timerTaskEntry 对象。
        timerTaskEntry.synchronized {
          // 如果在循环中检测到 timerTaskEntry 的 list 不为 null，
          // 则通过重新执行循环来重试添加操作，直到 timerTaskEntry 的 list 变为 null。
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            val tail = root.prev
            timerTaskEntry.next = root
            timerTaskEntry.prev = tail
            timerTaskEntry.list = this
            // 把 timerTaskEntry 添加到链表末尾
            tail.next = timerTaskEntry
            root.prev = timerTaskEntry
            // 增加任务计数器的值。
            taskCounter.incrementAndGet()
            // 将标志变量 done 设置为 true，表示添加操作完成。
            done = true
          }
        }
      }
    }
  }

  // Remove the specified timer task entry from this list
  // 从列表中移除一个 TimerTaskEntry
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        if (timerTaskEntry.list eq this) {
          // 从列表中移除 timerTaskEntry
          timerTaskEntry.next.prev = timerTaskEntry.prev
          timerTaskEntry.prev.next = timerTaskEntry.next
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          timerTaskEntry.list = null
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      // 找到链表第一个元素
      var head = root.next
      // 开始遍历链表
      while (head ne root) {
        // 移除遍历到的链表元素
        remove(head)
        // 执行传入参数f的逻辑
        f(head)
        head = root.next
      }
      // 清空过期时间设置
      expiration.set(-1L)
    }
  }

  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - Time.SYSTEM.hiResClockMs, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[TimerTaskList]
    java.lang.Long.compare(getExpiration, other.getExpiration)
  }

}

// timerTask 与该 TimerTaskEntry 相关联的 TimerTask 定时任务。
// expirationMs TimerTaskEntry 的过期时间。
private[timer] class TimerTaskEntry(val timerTask: TimerTask, val expirationMs: Long) extends Ordered[TimerTaskEntry] {

  @volatile
  // 当前 TimerTaskEntry 所在的 TimerTaskList
  var list: TimerTaskList = null
  var next: TimerTaskEntry = null
  var prev: TimerTaskEntry = null

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  // 如果 timerTask 不为 null，关联给定的定时任务
  if (timerTask != null) timerTask.setTimerTaskEntry(this)

  // 关联定时任务是否已经被取消了
  def cancelled: Boolean = {
    // 返回 timerTask.getTimerTaskEntry 是否等于当前 TimerTaskEntry，用于检查 TimerTask 是否已被取消。
    timerTask.getTimerTaskEntry != this
  }

  // 将 TimerTask 自身从双向链表中移除掉。
  def remove(): Unit = {
    var currentList = list
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
    while (currentList != null) {
      // 从所在的 TimerTaskList 中移除当前 TimerTaskEntry。
      currentList.remove(this)
      // 使用循环重试直到列表变为 null。
      currentList = list
    }
  }


  // 实现 Ordered trait 的 compare 方法，用于比较两个 TimerTaskEntry 的过期时间，以便进行排序。
  override def compare(that: TimerTaskEntry): Int = {
    java.lang.Long.compare(expirationMs, that.expirationMs)
  }
}

