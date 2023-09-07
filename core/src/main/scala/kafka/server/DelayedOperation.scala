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

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 *
 * Noted that if you add a future delayed operation that calls ReplicaManager.appendRecords() in onComplete()
 * like DelayedJoin, you must be aware that this operation's onExpiration() needs to call actionQueue.tryCompleteAction().
 */
abstract class DelayedOperation(override val delayMs: Long,
                                lockOpt: Option[Lock] = None)
  extends TimerTask with Logging {

  // 标识该延迟操作是否已经完成
  private val completed = new AtomicBoolean(false)
  // Visible for testing
  // 防止多个线程同时检查操作是否可完成时发生锁竞争导致操作最终超时
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   */
  def forceComplete(): Boolean = {
    // 使用 compareAndSet 方法比较并设置 completed 的值。如果 completed 的值尚未被设置为 true，则将其设置为 true，
    // 并继续执行以下操作。否则，直接返回 false。
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      // 在成功将 completed 的值设置为 true 后，执行以下操作：
      // 1、取消超时计时器。调用 cancel() 方法取消任务的超时计时器。
      // 2、执行完成时的操作。调用 onComplete() 方法执行任务完成时的操作。
      // 返回 true，表示任务成功标记为完成状态。
      cancel()
      onComplete()
      true
    } else {
      // 如果 completed 的值在调用 compareAndSet 方法之前已经是 true，则直接返回 false，表示任务无法强制标记为完成状态。
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   */
  // 检查延迟操作是否已经完成来决定后续如何处理该操作。比如如果操作已经完成了，那么通常需要取消该操作。
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   */
  // 强制完成之后执行的过期逻辑回调方法。只有真正完成操作的那个线程才有资格调用这个方法。
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   */
  // 完成延迟操作所需的处理逻辑。该方法只会在 forceComplete 方法中被调用。
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   */
  // 尝试完成延迟操作的顶层方法，内部会调用forceComplete方法
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete() and call extra function if first tryComplete returns false
   * @param f else function to be executed after first tryComplete returns false
   * @return result of tryComplete
   */
  // 以安全的方式尝试完成任务，否则执行提供的函数
  private[server] def safeTryCompleteOrElse(f: => Unit): Boolean = inLock(lock) {
    if (tryComplete()) true
    else {
      f
      // last completion check
      // 最后一次完成检查
      tryComplete()
    }
  }

  /**
   * Thread-safe variant of tryComplete()
   */
  // 线程安全版本的tryComplete方法
  private[server] def safeTryComplete(): Boolean = inLock(lock)(tryComplete())

  /*
   * run() method defines a task that is executed on timeout
   */
  // 重写 run 方法，用于执行定时任务
  override def run(): Unit = {
    // 强制标记任务为完成状态
    if (forceComplete())
    // 在任务超时时执行的操作
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  private val Shards = 512 // Shard the watcher list to reduce lock contention

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {
  /* a list of operation watching keys */
  private class WatcherList {
    // 定义一组按照 Key 分组的 Watchers 对象
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     */
    // 返回所有 Watchers 对象
    def allWatchers = {
      watchersByKey.values
    }
  }

  private val watcherLists = Array.fill[WatcherList](DelayedOperationPurgatory.Shards)(new WatcherList)
  private def watcherList(key: Any): WatcherList = {
    watcherLists(Math.abs(key.hashCode() % watcherLists.length))
  }

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)
  newGauge("PurgatorySize", () => watched, metricsTags)
  newGauge("NumDelayedOperations", () => numDelayed, metricsTags)

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling tryComplete() for each key is
    // going to be expensive if there are many keys. Instead, we do the check in the following way through safeTryCompleteOrElse().
    // If the operation is not completed, we just add the operation to all keys. Then we call tryComplete() again. At
    // this time, if the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys.
    //
    // ==============[story about lock]==============
    // Through safeTryCompleteOrElse(), we hold the operation's lock while adding the operation to watch list and doing
    // the tryComplete() check. This is to avoid a potential deadlock between the callers to tryCompleteElseWatch() and
    // checkAndComplete(). For example, the following deadlock can happen if the lock is only held for the final tryComplete()
    // 1) thread_a holds readlock of stateLock from TransactionStateManager
    // 2) thread_a is executing tryCompleteElseWatch()
    // 3) thread_a adds op to watch list
    // 4) thread_b requires writelock of stateLock from TransactionStateManager (blocked by thread_a)
    // 5) thread_c calls checkAndComplete() and holds lock of op
    // 6) thread_c is waiting readlock of stateLock to complete op (blocked by thread_b)
    // 7) thread_a is waiting lock of op to call the final tryComplete() (blocked by thread_c)
    //
    // Note that even with the current approach, deadlocks could still be introduced. For example,
    // 1) thread_a calls tryCompleteElseWatch() and gets lock of op
    // 2) thread_a adds op to watch list
    // 3) thread_a calls op#tryComplete and tries to require lock_b
    // 4) thread_b holds lock_b and calls checkAndComplete()
    // 5) thread_b sees op from watch list
    // 6) thread_b needs lock of op
    // To avoid the above scenario, we recommend DelayedOperationPurgatory.checkAndComplete() be called without holding
    // any exclusive lock. Since DelayedOperationPurgatory.checkAndComplete() completes delayed operations asynchronously,
    // holding a exclusive lock to make the call is often unnecessary.
    // 尝试完成操作，如果操作已经完成，则返回 true
    if (operation.safeTryCompleteOrElse {
      // 对于每个 watchKey，将操作添加到对应的监控列表中
      watchKeys.foreach(key => watchForOperation(key, operation))
      // 如果有 watchKey，则增加估计的总操作数
      if (watchKeys.nonEmpty) estimatedTotalOperations.incrementAndGet()
    }) return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    // 如果依然不能完成此请求，将其加入到过期队列
    if (!operation.isCompleted) {
      // 如果启用了定时器，则将操作添加到超时定时器中
      if (timerEnabled)
        timeoutTimer.add(operation)
      if (operation.isCompleted) {
        // cancel the timer task
        // 如果操作在添加到定时器之前已经完成，则取消定时器任务
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    // 获取给定 Key 的 WatcherList
    val wl = watcherList(key)
    // 获取 WatcherList 中 Key 对应的 Watchers 对象实例
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    // 尝试完成满足完成条件的延迟请求并返回成功完成的请求数
    val numCompleted = if (watchers == null)
      0
    else
      watchers.tryCompleteWatched()
    debug(s"Request key $key unblocked $numCompleted $purgatoryName operations")
    numCompleted
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = {
    watcherLists.foldLeft(0) { case (sum, watcherList) => sum + watcherList.allWatchers.map(_.countWatched).sum }
  }

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def numDelayed: Int = timeoutTimer.size

  /**
    * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
    */
  def cancelForKey(key: Any): List[T] = {
    // 根据 key 获取对应的监控列表 wl。
    val wl = watcherList(key)
    // 使用 wl 的 watchersLock 进行同步操作，确保线程安全
    inLock(wl.watchersLock) {
      // 从 wl 的 watchersByKey 中移除 key 并获取与之关联的监控操作列表 watchers
      val watchers = wl.watchersByKey.remove(key)
      // 如果监控操作列表 watchers 不为空，即存在与 key 关联的监控操作
      if (watchers != null)
      // 取消所有监控操作
        watchers.cancel()
      else
        Nil
    }
  }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T): Unit = {
    // 获取 key 对应的监控列表 wl
    val wl = watcherList(key)
    // 使用 wl 的 watchersLock 进行同步操作
    inLock(wl.watchersLock) {
      // 获取 key 对应的监控器 watcher，如果不存在则创建一个新的监控器并加入监控列表
      val watcher = wl.watchersByKey.getAndMaybePut(key)
      // 将操作添加到监控器中进行监控
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers): Unit = {
    // 获取 key 对应的监控列表 wl
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (wl.watchersByKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        wl.watchersByKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown(): Unit = {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
    removeMetric("PurgatorySize", metricsTags)
    removeMetric("NumDelayedOperations", metricsTags)
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    // 计算当前正在监控的操作数量。这是一个 O(n) 的操作，如果可能，应使用 isEmpty() 方法来检查是否为空。
    def countWatched: Int = operations.size

    // 检查监视列表是否为空
    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    // 添加要监控的元素
    def watch(t: T): Unit = {
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    // 遍历监控列表，并尝试完成一些已监控的元素
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          // 其他线程已经完成了这个操作，只需将其移除
          iter.remove()
        } else if (curr.safeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      // 如果监控列表为空，则从全局列表中移除该键
      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    // 取消监控列表中的所有元素
    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // traverse the list and purge elements that are already completed by others
    // 遍历监控列表，并清除已被其他线程完成的元素
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      // 如果监控列表为空，则从全局列表中移除该键
      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long): Unit = {
    // 推进超时定时器的时钟到 timeoutMs
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      // 如果已完成但仍在监控中的操作数（estimatedTotalOperations - numDelayed）大于清理阈值（purgeInterval）if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // 将 estimatedTotalOperations 重新设置为待处理操作（numDelayed）的数量，因为接下来要清理监控列表
      estimatedTotalOperations.getAndSet(numDelayed)
      debug("Begin purging watch lists")
      val purged = watcherLists.foldLeft(0) {
        // 对所有监控列表进行遍历，并清理已经完成的操作val purged = watcherLists.foldLeft(0) {
        case (sum, watcherList) => sum + watcherList.allWatchers.map(_.purgeCompleted()).sum
      }
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  // 创建一个 ExpiredOperationReaper 实例作为清理线程，并定义线程名private val expirationReaper = new ExpiredOperationReaper()
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork(): Unit = {
      // 重写 ShutdownableThread 中的 doWork() 方法，定义清理线程的具体工作任务override def doWork(): Unit = {
      advanceClock(200L)
    }
  }
}
