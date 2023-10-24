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

package kafka.coordinator.group

import java.util

import kafka.utils.nonthreadsafe

case class MemberSummary(
                        // 用来标识消费者组成员的 ID，它是由 Kafka 自动生成的，规则是 consumer-组ID-<序号>-。目前是硬编码的，无法自己设置。
                        memberId: String,
                        // 用来标识消费者组静态成员的 ID。「静态成员机制」的引入能够规避不必要的消费者组Rebalance 操作。
                         groupInstanceId: Option[String],
                        // 用来标识消费者组成员配置的 client.id 参数。由于 memberId 是硬编码的无法被设置，
                        // 所以你可以用该字段来区分消费者组下的不同成员。
                         clientId: String,
                        // 用来标识运行消费者主机名。它记录了该客户端是从哪台机器发出的消费请求。
                         clientHost: String,
                        // 用来标识消费者组成员分区分配策略的字节数组，它是由消费者端参数 partition.assignment.strategy 值来设定，
                        // 默认分区分配策略为： RangeAssignor 策略。
                         metadata: Array[Byte],
                        // 用来保存分配给该成员的订阅分区。在前面讲过每个消费者组都要选出一个 Leader 消费者组成员负责给所有成员分配消费方案。
                        // 该字段就是用来使 Kafka 将制定好的分配方案序列化成字节数组，随后分发给各个组成员。
                         assignment: Array[Byte])

private object MemberMetadata {
  // 提取分区分配策略集合
  def plainProtocolSet(supportedProtocols: List[(String, Array[Byte])]) = supportedProtocols.map(_._1).toSet
}

/**
 * Member metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 */
@nonthreadsafe
private[group] class MemberMetadata(var memberId: String, // 用来标识消费者组成员的 ID
                                    val groupId: String,
                                    val groupInstanceId: Option[String], // 用来标识消费者组静态成员的 ID
                                    val clientId: String, // 用来标识消费者组成员配置的 client.id 参数
                                    val clientHost: String, // 用来标识运行消费者主机名
                                    val rebalanceTimeoutMs: Int,  // Rebalane 操作超时时间
                                    val sessionTimeoutMs: Int, // 会话超时时间
                                    val protocolType: String,  // 对消费者组来说就是是 "consumer"
                                    var supportedProtocols: List[(String, Array[Byte])] // 成员配置的多套分区分配策略
                                   ) {

  // 分区分配方案
  var assignment: Array[Byte] = Array.empty[Byte]
  // 组成员是否正在等待加入消费者组
  var awaitingJoinCallback: JoinGroupResult => Unit = null
  // 组成员是否正在等待 GroupCoordinator 发送分配方案
  var awaitingSyncCallback: SyncGroupResult => Unit = null

  var isLeaving: Boolean = false
  // 是否是消费者组下的新成员
  var isNew: Boolean = false
  // 是否是静态成员
  val isStaticMember: Boolean = groupInstanceId.isDefined

  // This variable is used to track heartbeat completion through the delayed
  // heartbeat purgatory. When scheduling a new heartbeat expiration, we set
  // this value to `false`. Upon receiving the heartbeat (or any other event
  // indicating the liveness of the client), we set it to `true` so that the
  // delayed heartbeat can be completed.
  // 当心跳过期时设置为false,接收到心跳设置为true
  var heartbeatSatisfied: Boolean = false

  // 是否正在等待加入消费者组
  def isAwaitingJoin = awaitingJoinCallback != null
  // 是否正在等待 GroupCoordinator 发送分配方案
  def isAwaitingSync = awaitingSyncCallback != null

  /**
   * Get metadata corresponding to the provided protocol.
   */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  // 用来检查成员是否满足预期的心跳
  def hasSatisfiedHeartbeat: Boolean = {
    // 如果成员状态为 isNew，则首先检查 heartbeatSatisfied 是否为 true。
    if (isNew) {
      // New members can be expired while awaiting join, so we have to check this first
      heartbeatSatisfied
      // 如果成员状态为 isAwaitingJoin 或 isAwaitingSync，则自动满足预期的心跳要求，返回 true。
    } else if (isAwaitingJoin || isAwaitingSync) {
      // Members that are awaiting a rebalance automatically satisfy expected heartbeats
      true
    } else {
      // Otherwise we require the next heartbeat
      // 否则需要下一个心跳才能满足预期，返回 heartbeatSatisfied 的值。
      heartbeatSatisfied
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   */
  // 用来检查提供的协议元数据是否与当前存储的元数据匹配
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    // 首先检查提供的协议元数据的 size 是否与当前支持的协议元数据的 size 相等，如果不相等则返回false。
    if (protocols.size != this.supportedProtocols.size)
      return false

    // 逐个比较提供的协议元数据和当前支持的协议元数据
    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      // 如果协议名称不匹配，或者协议元数据内容不相等则返回 false。
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    // 如果上述比较都通过，则说明提供的协议元数据匹配当前存储的元数据返回 true。
    true
  }

  def summary(protocol: String): MemberSummary = {
    // 根据提供的协议，创建成员的摘要信息并返回。摘要信息包括成员的ID、群组实例ID、客户端ID、客户端主机、协议对应的元数据和分配。
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    // 创建没有元数据的成员摘要信息并返回。摘要信息包括成员的ID、群组实例ID、客户端ID、客户端主机，但不包含任何元数据和分配。
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   */
  // 用来为潜在的组协议进行投票。根据支持的协议的顺序和候选协议的集合，确定协议首选项，并返回第一个同时在集合中的协议
  def vote(candidates: Set[String]): String = {
    // 在支持的协议中查找第一个同时出现在候选协议集合中的协议
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      // 如果找到了匹配的协议，则返回该协议。
      case Some((protocol, _)) => protocol
      // 如果找不到匹配的协议，则抛出异常。说明成员不支持任何候选协议。
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"groupInstanceId=$groupInstanceId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +
      ")"
  }
}
