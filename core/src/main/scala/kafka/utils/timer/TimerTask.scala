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

trait TimerTask extends Runnable {

  // request.timeout.ms 参数值
  val delayMs: Long // timestamp in millisecond

  // 每个 TimerTask 实例关联一个 TimerTaskEntry，每个定时任务需要知道它在哪个 Bucket 链表下的哪个链表元素上
  private[this] var timerTaskEntry: TimerTaskEntry = null

  // 取消定时任务
  def cancel(): Unit = {
    synchronized {
      // timerTaskEntry 不为空，则把定时任务从链表上移除。
      if (timerTaskEntry != null) timerTaskEntry.remove()
      // 将关联的 timerTaskEntry 置空
      timerTaskEntry = null
    }
  }

  // 关联timerTaskEntry，由锁保护起来，以保证线程安全性
  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      // 判断定时任务是否已经绑定了其他的 timerTaskEntry，如果是的话，就必须先取消绑定
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      // 给 timerTaskEntry 字段赋值
      timerTaskEntry = entry
    }
  }

  // 获取关联的 timerTaskEntry 实例
  private[timer] def getTimerTaskEntry: TimerTaskEntry = timerTaskEntry

}
