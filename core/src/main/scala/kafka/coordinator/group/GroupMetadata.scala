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
package kafka.coordinator.group

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import kafka.common.OffsetAndMetadata
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.Time

import scala.collection.{Seq, immutable, mutable}
import scala.jdk.CollectionConverters._

private[group] sealed trait GroupState {
  val validPreviousStates: Set[GroupState]
}

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 *             all members have left the group => Empty
 *             group is removed by partition emigration => Dead
 */
private[group] case object PreparingRebalance extends GroupState {
  // 合法前置状态，可以从 Stable、CompletingRebalance、Empty 状态进行转换
  val validPreviousStates: Set[GroupState] = Set(Stable, CompletingRebalance, Empty)
}

/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object CompletingRebalance extends GroupState {
  // 合法前置状态，只可以从 PreparingRebalance 状态进行转换
  val validPreviousStates: Set[GroupState] = Set(PreparingRebalance)
}

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object Stable extends GroupState {
  // 合法前置状态，只可以从 CompletingRebalance 状态进行转换
  val validPreviousStates: Set[GroupState] = Set(CompletingRebalance)
}

/**
 * Group has no more members and its metadata is being removed
 *
 * action: respond to join group with UNKNOWN_MEMBER_ID
 *         respond to sync group with UNKNOWN_MEMBER_ID
 *         respond to heartbeat with UNKNOWN_MEMBER_ID
 *         respond to leave group with UNKNOWN_MEMBER_ID
 *         respond to offset commit with UNKNOWN_MEMBER_ID
 *         allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[group] case object Dead extends GroupState {
  // 合法前置状态，可以从所有状态进行转换
  val validPreviousStates: Set[GroupState] = Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead)
}

/**
  * Group has no more members, but lingers until all offsets have expired. This state
  * also represents groups which use Kafka only for offset commits and have no members.
  *
  * action: respond normally to join group from new members
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * transition: last offsets removed in periodic expiration task => Dead
  *             join group from a new member => PreparingRebalance
  *             group is removed by partition emigration => Dead
  *             group is removed by expiration => Dead
  */
private[group] case object Empty extends GroupState {
  // 合法前置状态，只可以从 PreparingRebalance 状态进行转换
  val validPreviousStates: Set[GroupState] = Set(PreparingRebalance)
}


private object GroupMetadata extends Logging {

  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocolName: String,
                leaderId: String,
                currentStateTimestamp: Option[Long],
                members: Iterable[MemberMetadata],
                time: Time): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState, time)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocolName = Option(protocolName)
    group.leaderId = Option(leaderId)
    group.currentStateTimestamp = currentStateTimestamp
    members.foreach(member => {
      group.add(member, null)
      if (member.isStaticMember) {
        info(s"Static member $member.groupInstanceId of group $groupId loaded " +
          s"with member id ${member.memberId} at generation ${group.generationId}.")
        group.addStaticMember(member.groupInstanceId, member.memberId)
      }
    })
    group.subscribedTopics = group.computeSubscribedTopics()
    group
  }

  private val MemberIdDelimiter = "-"
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String, // 组ID信息，即group.id参数值
                         protocolType: String, // 消费者组的协议类型
                         state: String)  // 消费者组的状态

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String,        // 消费者组状态
                        protocolType: String, // 协议类型
                        protocol: String,     // 消费者组选定的分区分配策略
                        members: List[MemberSummary])  // 成员元数据

/**
  * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
  * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
  * information of the commit record offset, compaction of the offsets topic itself may result in the wrong offset commit
  * being materialized.
  */
// appendedBatchOffset：其内部保存的是位移主题消息自己的位移值
// offsetAndMetadata：其内部保存的是位移提交消息中保存的消费者组的位移值。
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset): Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 */
@nonthreadsafe
private[group] class GroupMetadata(val groupId: String, initialState: GroupState, time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit

  private[group] val lock = new ReentrantLock

  // 初始消费者组状态
  private var state: GroupState = initialState
  // 记录状态最近一次变更的时间戳
  var currentStateTimestamp: Option[Long] = Some(time.milliseconds())
  var protocolType: Option[String] = None
  var protocolName: Option[String] = None
  // 消费组 Generation 号。Generation 等同于消费者组执行过 Rebalance 操作的次数，每次执行 Rebalance时，Generation 数都要加 1。
  var generationId = 0
  // 消费者组中 Leader 成员的 Member ID 信息。当消费者组执行 Rebalance 过程时，需要选举一个成员作为Leader，负责为所有成员制定分区分配方案。
  // 在 Rebalance 早期阶段，这个 Leader 可能尚未被选举出来则为 None。
  private var leaderId: Option[String] = None

  // 保存消费者组下所有成员的元数据列表信息。
  private val members = new mutable.HashMap[String, MemberMetadata]
  // Static membership mapping [key: group.instance.id, value: member.id]
  // 静态成员 Id 列表。
  private val staticMembers = new mutable.HashMap[String, String]
  // 待加入的成员 Id 列表。
  private val pendingMembers = new mutable.HashSet[String]
  // 正在等待加入 Group 的成员数量。
  private var numMembersAwaitingJoin = 0
  // 保存分区分配策略的支持票数。它是一个 HashMap 类型，其中，Key 是分配策略的名称，Value是 支持的票数。
  private val supportedProtocols = new mutable.HashMap[String, Integer]().withDefaultValue(0)
  // 保存消费者组订阅分区的提交位移值
  // 保存按照主题分区分组的位移主题消息位移值的 HashMap。其中 Key 是主题分区，Value 是CommitRecordMetadataAndOffset 类型。
  // 当消费者组成员向 Kafka 提交位移时，插入对应的记录到该字段。
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  // 保存待提交的偏移量。
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  // 标志是否已接收到消费者的提交的偏移量。
  private var receivedConsumerOffsetCommits = false

  // When protocolType == `consumer`, a set of subscribed topics is maintained. The set is
  // computed when a new generation is created or when the group is restored from the log.
  // 保存消费者组订阅的主题列表，用于帮助从offsets字段中过滤订阅主题分区的位移值。
  private var subscribedTopics: Option[Set[String]] = None

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  // 判断消费者组状态是指定状态
  def is(groupState: GroupState) = state == groupState
  // 判断消费者组状态不是指定状态
  def not(groupState: GroupState) = state != groupState
  // 判断当前消费者组是否包含该指定成员
  def has(memberId: String) = members.contains(memberId)
  // 获取当前消费者组该指定成员
  def get(memberId: String) = members(memberId)
  // 统计当前消费者组总成员数
  def size = members.size

  // 判断 LeaderId 成员中是否包含该指定成员
  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)
  def leaderOrNull: String = leaderId.orNull
  def currentStateTimestampOrDefault: Long = currentStateTimestamp.getOrElse(-1)

  def isConsumerGroup: Boolean = protocolType.contains(ConsumerProtocol.PROTOCOL_TYPE)

  def add(member: MemberMetadata, callback: JoinCallback = null): Unit = {
    // 如果是要添加的第一个消费者组成员
    if (members.isEmpty)
      // 把该成员的 procotolType 设置为该消费者组的 protocolType
      this.protocolType = Some(member.protocolType)

    assert(groupId == member.groupId)
    // 确保成员元数据中的 protoclType 和组 protocolType 相同
    assert(this.protocolType.orNull == member.protocolType)
    // 确保该成员选定的分区分配策略与组选定的分区分配策略相匹配
    assert(supportsProtocols(member.protocolType, MemberMetadata.plainProtocolSet(member.supportedProtocols)))

    // 如果此时还未选出消费者组的 Leader 成员
    if (leaderId.isEmpty)
    // 把该成员选为 Leader 成员
      leaderId = Some(member.memberId)
    // 将该成员添加进 members
    members.put(member.memberId, member)
    // 递增分区分配策略支持票数
    member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) += 1 }
    // 设置成员加入消费者组后的回调方法
    member.awaitingJoinCallback = callback
    // 递增已加入消费者组的成员数
    if (member.isAwaitingJoin)
      numMembersAwaitingJoin += 1
  }

  def remove(memberId: String): Unit = {
    // 从 members 中移除给定成员
    members.remove(memberId).foreach { member =>
      // 递减分区分配策略支持票数
      member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) -= 1 }
      // 递减已加入消费者组的成员数
      if (member.isAwaitingJoin)
        numMembersAwaitingJoin -= 1
    }

    // 如果该成员是Leader，选择剩下成员列表中的第一个作为新的Leader成员
    if (isLeader(memberId))
      leaderId = members.keys.headOption
  }

  /**
    * Check whether current leader is rejoined. If not, try to find another joined member to be
    * new leader. Return false if
    *   1. the group is currently empty (has no designated leader)
    *   2. no member rejoined
    */
  def maybeElectNewJoinedLeader(): Boolean = {
    leaderId.exists { currentLeaderId =>
      val currentLeader = get(currentLeaderId)
      if (!currentLeader.isAwaitingJoin) {
        members.find(_._2.isAwaitingJoin) match {
          case Some((anyJoinedMemberId, anyJoinedMember)) =>
            leaderId = Option(anyJoinedMemberId)
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, while new leader $anyJoinedMember was elected.")
            true

          case None =>
            info(s"Group leader [member.id: ${currentLeader.memberId}, " +
              s"group.instance.id: ${currentLeader.groupInstanceId}] failed to join " +
              s"before rebalance timeout, and the group couldn't proceed to next generation" +
              s"because no member joined.")
            false
        }
      } else {
        true
      }
    }
  }

  /**
    * [For static members only]: Replace the old member id with the new one,
    * keep everything else unchanged and return the updated member.
    */
  def replaceGroupInstance(oldMemberId: String,
                           newMemberId: String,
                           groupInstanceId: Option[String]): MemberMetadata = {
    if(groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in replaceGroupInstance")
    }
    val oldMember = members.remove(oldMemberId)
      .getOrElse(throw new IllegalArgumentException(s"Cannot replace non-existing member id $oldMemberId"))

    // Fence potential duplicate member immediately if someone awaits join/sync callback.
    maybeInvokeJoinCallback(oldMember, JoinGroupResult(oldMemberId, Errors.FENCED_INSTANCE_ID))

    maybeInvokeSyncCallback(oldMember, SyncGroupResult(Errors.FENCED_INSTANCE_ID))

    oldMember.memberId = newMemberId
    members.put(newMemberId, oldMember)

    if (isLeader(oldMemberId))
      leaderId = Some(newMemberId)
    addStaticMember(groupInstanceId, newMemberId)
    oldMember
  }

  // 检查指定的成员ID是否是待加入的成员
  def isPendingMember(memberId: String): Boolean = pendingMembers.contains(memberId) && !has(memberId)

  def addPendingMember(memberId: String) = pendingMembers.add(memberId)

  def removePendingMember(memberId: String) = pendingMembers.remove(memberId)

  def hasStaticMember(groupInstanceId: Option[String]) = groupInstanceId.isDefined && staticMembers.contains(groupInstanceId.get)

  def getStaticMemberId(groupInstanceId: Option[String]) = {
    if(groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in getStaticMemberId")
    }
    staticMembers(groupInstanceId.get)
  }

  def addStaticMember(groupInstanceId: Option[String], newMemberId: String) = {
    if(groupInstanceId.isEmpty) {
      throw new IllegalArgumentException(s"unexpected null group.instance.id in addStaticMember")
    }
    staticMembers.put(groupInstanceId.get, newMemberId)
  }

  def removeStaticMember(groupInstanceId: Option[String]) = {
    if (groupInstanceId.isDefined) {
      staticMembers.remove(groupInstanceId.get)
    }
  }

  // 查询当前状态
  def currentState = state

  // 获取尚未重新加入消费者组的成员的元数据信息。
  def notYetRejoinedMembers = members.filter(!_._2.isAwaitingJoin).toMap

  // 检查是否所有成员已加入消费者组。
  def hasAllMembersJoined = members.size == numMembersAwaitingJoin && pendingMembers.isEmpty

  def allMembers = members.keySet

  def allStaticMembers = staticMembers.keySet

  // For testing only.
  def allDynamicMembers = {
    val dynamicMemberSet = new mutable.HashSet[String]
    allMembers.foreach(memberId => dynamicMemberSet.add(memberId))
    staticMembers.values.foreach(memberId => dynamicMemberSet.remove(memberId))
    dynamicMemberSet.toSet
  }

  // 获取待加入的成员数量
  def numPending = pendingMembers.size

  // 获取正在等待加入的成员数量
  def numAwaiting: Int = numMembersAwaitingJoin

  // 返回消费者组中所有成员的元数据信息的列表
  def allMemberMetadata = members.values.toList

  // 获取消费者组中所有成员的最大重新平衡超时时间
  def rebalanceTimeoutMs = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs)
  }

  def generateMemberId(clientId: String,
                       groupInstanceId: Option[String]): String = {
    groupInstanceId match {
      case None =>
        clientId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
      case Some(instanceId) =>
        instanceId + GroupMetadata.MemberIdDelimiter + UUID.randomUUID().toString
    }
  }

  /**
    * Verify the member.id is up to date for static members. Return true if both conditions met:
    *   1. given member is a known static member to group
    *   2. group stored member.id doesn't match with given member.id
    */
  // 检查指定的消费者组实例ID的静态成员是否被拒绝访问
  def isStaticMemberFenced(memberId: String,
                           groupInstanceId: Option[String],
                           operation: String): Boolean = {
    if (hasStaticMember(groupInstanceId)
      && getStaticMemberId(groupInstanceId) != memberId) {
      error(s"given member.id $memberId is identified as a known static member ${groupInstanceId.get}, " +
        s"but not matching the expected member.id ${getStaticMemberId(groupInstanceId)} during $operation, will " +
        s"respond with instance fenced error")
      true
    } else
      false
  }

  // 消费者组能否 Rebalance 的条件是当前状态是 PreparingRebalance 状态包含的合法前置状态
  def canRebalance = PreparingRebalance.validPreviousStates.contains(state)

  // 设置/更新状态
  def transitionTo(groupState: GroupState): Unit = {
    // 确保是合法的状态转换
    assertValidTransition(groupState)
    // 设置状态到给定状态
    state = groupState
    // 更新状态变更时间戳
    currentStateTimestamp = Some(time.milliseconds())
  }

  def selectProtocol: String = {
    // 如果没有任何成员，直接抛异常
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    // 获取所有成员都支持的策略集合
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    // 让每个成员投票，票数最多的那个策略当选
    val (protocol, _) = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .maxBy { case (_, votes) => votes.size }

    protocol
  }

  private def candidateProtocols: Set[String] = {
    // get the set of protocols that are commonly supported by all members
    // 获取消费者组内成员数
    val numMembers = members.size
    // 该消费者组中所有成员都支持该分区分配策略，并返回它们的名称
    supportedProtocols.filter(_._2 == numMembers).map(_._1).toSet
  }

  def supportsProtocols(memberProtocolType: String, memberProtocols: Set[String]): Boolean = {
    if (is(Empty))
      !memberProtocolType.isEmpty && memberProtocols.nonEmpty
    else
      protocolType.contains(memberProtocolType) && memberProtocols.exists(supportedProtocols(_) == members.size)
  }

  def getSubscribedTopics: Option[Set[String]] = subscribedTopics

  /**
   * Returns true if the consumer group is actively subscribed to the topic. When the consumer
   * group does not know, because the information is not available yet or because the it has
   * failed to parse the Consumer Protocol, it returns true to be safe.
   */
  def isSubscribedToTopic(topic: String): Boolean = subscribedTopics match {
    case Some(topics) => topics.contains(topic)
    case None => true
  }

  /**
   * Collects the set of topics that the members are subscribed to when the Protocol Type is equal
   * to 'consumer'. None is returned if
   * - the protocol type is not equal to 'consumer';
   * - the protocol is not defined yet; or
   * - the protocol metadata does not comply with the schema.
   */
  private[group] def computeSubscribedTopics(): Option[Set[String]] = {
    protocolType match {
      case Some(ConsumerProtocol.PROTOCOL_TYPE) if members.nonEmpty && protocolName.isDefined =>
        try {
          Some(
            members.map { case (_, member) =>
              // The consumer protocol is parsed with V0 which is the based prefix of all versions.
              // This way the consumer group manager does not depend on any specific existing or
              // future versions of the consumer protocol. VO must prefix all new versions.
              val buffer = ByteBuffer.wrap(member.metadata(protocolName.get))
              ConsumerProtocol.deserializeVersion(buffer)
              ConsumerProtocol.deserializeSubscription(buffer, 0).topics.asScala.toSet
            }.reduceLeft(_ ++ _)
          )
        } catch {
          case e: SchemaException =>
            warn(s"Failed to parse Consumer Protocol ${ConsumerProtocol.PROTOCOL_TYPE}:${protocolName.get} " +
              s"of group $groupId. Consumer group coordinator is not aware of the subscribed topics.", e)
            None
        }

      case Some(ConsumerProtocol.PROTOCOL_TYPE) if members.isEmpty =>
        Option(Set.empty)

      case _ => None
    }
  }

  def updateMember(member: MemberMetadata,
                   protocols: List[(String, Array[Byte])],
                   callback: JoinCallback): Unit = {
    member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) -= 1 }
    protocols.foreach{ case (protocol, _) => supportedProtocols(protocol) += 1 }
    member.supportedProtocols = protocols

    if (callback != null && !member.isAwaitingJoin) {
      numMembersAwaitingJoin += 1
    } else if (callback == null && member.isAwaitingJoin) {
      numMembersAwaitingJoin -= 1
    }
    member.awaitingJoinCallback = callback
  }

  def maybeInvokeJoinCallback(member: MemberMetadata,
                              joinGroupResult: JoinGroupResult): Unit = {
    if (member.isAwaitingJoin) {
      member.awaitingJoinCallback(joinGroupResult)
      member.awaitingJoinCallback = null
      numMembersAwaitingJoin -= 1
    }
  }

  /**
    * @return true if a sync callback actually performs.
    */
  def maybeInvokeSyncCallback(member: MemberMetadata,
                              syncGroupResult: SyncGroupResult): Boolean = {
    if (member.isAwaitingSync) {
      member.awaitingSyncCallback(syncGroupResult)
      member.awaitingSyncCallback = null
      true
    } else {
      false
    }
  }

  def initNextGeneration(): Unit = {
    if (members.nonEmpty) {
      generationId += 1
      protocolName = Some(selectProtocol)
      subscribedTopics = computeSubscribedTopics()
      transitionTo(CompletingRebalance)
    } else {
      generationId += 1
      protocolName = None
      subscribedTopics = computeSubscribedTopics()
      transitionTo(Empty)
    }
    receivedConsumerOffsetCommits = false
    receivedTransactionalOffsetCommits = false
  }

  def currentMemberMetadata: List[JoinGroupResponseMember] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map{ case (memberId, memberMetadata) => new JoinGroupResponseMember()
        .setMemberId(memberId)
        .setGroupInstanceId(memberMetadata.groupInstanceId.orNull)
        .setMetadata(memberMetadata.metadata(protocolName.get))
    }.toList
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val protocol = protocolName.orNull
      if (protocol == null)
        throw new IllegalStateException("Invalid null group protocol for stable group")

      val members = this.members.values.map { member => member.summary(protocol) }
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members.toList)
    } else {
      val members = this.members.values.map{ member => member.summaryNoMetadata() }
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members.toList)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType.getOrElse(""), state.toString)
  }

  // 初始化偏移量和待处理的事务性偏移量提交
  def initializeOffsets(offsets: collection.Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTxnOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]): Unit = {
    // 将传入的偏移量集合累计到 offsets 变量中
    this.offsets ++= offsets
    // 将传入的待处理事务性偏移量提交集合添加到 pendingTransactionalOffsetCommits 变量中
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets
  }

  // 在提交位移消息被成功写入后调用
  def onOffsetCommitAppend(topicPartition: TopicPartition, offsetWithCommitRecordMetadata: CommitRecordMetadataAndOffset): Unit = {
    // 如果 pendingOffsetCommits 中包含该分区的偏移量提交记录
    if (pendingOffsetCommits.contains(topicPartition)) {
      // 如果 offsetWithCommitRecordMetadata.appendedBatchOffset 为空，抛出异常。因为没有提供日志中记录的元数据，无法完成偏移量提交的写入。
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +
          "in the log.")
      // 如果 offsets 不包含该分区位移提交数据，或者 offsets 中该分区对应的提交位移消息在位移主题中的位移值小于待写入的位移值
      if (!offsets.contains(topicPartition) || offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata))
      // 将该分区对应的提交位移消息添加到 offsets 中
        offsets.put(topicPartition, offsetWithCommitRecordMetadata)
    }

    // 成功处理偏移量的提交记录后，更新和管理偏移量的状态，并确保偏移量的正确提交和删除
    pendingOffsetCommits.get(topicPartition) match {
      // 如果相同，表示偏移量已成功提交，从 pendingOffsetCommits 中删除该分区的偏移量提交记录。
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition)
      case _ =>
      // 如果不相同，则保留 pendingOffsetCommits 中的该分区的偏移量提交记录，并且如果该分区的主题已被删除，
      // 则它的条目将由 removeOffsets 方法从缓存中删除。
        // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
        // its entries would be removed from the cache by the `removeOffsets` method.
    }
  }

  def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    receivedConsumerOffsetCommits = true
    pendingOffsetCommits ++= offsets
  }

  def prepareTxnOffsetCommit(producerId: Long, offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])

    offsets.forKeyValue { (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata))
    }
  }

  def hasReceivedConsistentOffsetCommits : Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
        // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
        // We may hit this case if the partition in question has emigrated.
    }
  }

  /* Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   */
  // 正在进行中、还没有完成的事务提交
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    // 获取指定producerId的挂起事务性偏移量提交，并将其从pendingTransactionalOffsetCommits中移除
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    // 完成事务性偏移量提交
    if (isCommit) {
      // 检查每个挂起的偏移量提交记录
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.forKeyValue { (topicPartition, commitRecordMetadataAndOffset) =>
          // 如果commitRecordMetadataAndOffset.appendedBatchOffset为空，抛出异常。
          // 因为偏移量提交记录本身尚未添加到日志中，无法完成事务性偏移量的提交。
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          // 如果 offsets 不包含该分区位移提交数据，或者 offsets 中该分区对应的提交位移消息在位移主题中的位移值小于待写入的位移值
          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            // 将该分区对应的事务提交位移消息添加到 offsets 中
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            // 如果 isCommit 为 false，表示事务性偏移量提交被中止，记录相应的日志。
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers: collection.Set[Long] = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long): Boolean =
    pendingTransactionalOffsetCommits.contains(producerId)

  def hasPendingOffsetCommitsForTopicPartition(topicPartition: TopicPartition): Boolean = {
    pendingOffsetCommits.contains(topicPartition) ||
      pendingTransactionalOffsetCommits.exists(
        _._2.contains(topicPartition)
      )
  }

  def removeAllOffsets(): immutable.Map[TopicPartition, OffsetAndMetadata] = removeOffsets(offsets.keySet.toSeq)

  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.forKeyValue { (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  def removeExpiredOffsets(currentTimestamp: Long, offsetRetentionMs: Long): Map[TopicPartition, OffsetAndMetadata] = {

    // 专门用于获取订阅分区过期的位移值
    def getExpiredOffsets(baseTimestamp: CommitRecordMetadataAndOffset => Long,
                          subscribedTopics: Set[String] = Set.empty): Map[TopicPartition, OffsetAndMetadata] = {

      // 遍历 offsets 中的所有分区，过滤出同时满足以下 3 个条件的所有分区
      // 1、分区所属主题不在订阅主题列表之内
      // 2、该主题分区已经完成位移提交
      // 3、该主题分区在位移主题中对应消息的存在时间超过了阈值
      offsets.filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          // 分区所属主题不在订阅主题列表之内，对于正在消费的主题是不能执行过期位移移除的
          !subscribedTopics.contains(topicPartition.topic()) &&
            // 该主题分区已经完成位移提交，对于提交中状态的分区不能移除
          !pendingOffsetCommits.contains(topicPartition) && {
            // 该主题分区在位移主题中对应消息的存在时间超过了阈值，对于新版 Kafka 来说，判断是否过期主要基于消费者组状态来处理。
            // 如果是 Empty 状态，是否过期是根据当前时间与消费者组变为 Empty 状态时间的差值，
            // 是否超过 Broker 端参数 offsets.retention.minutes 值；如果不是 Empty 状态，
            // 是否过期是当前时间与提交位移消息中的时间戳差值是否超过了 offsets.retention.minutes 值。
            // 如果超过了，就认为已过期，对应的位移值需要被移除；如果没有超过，就不需要移除了。
            commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp match {
              case None =>
                // current version with no per partition retention
                currentTimestamp - baseTimestamp(commitRecordMetadataAndOffset) >= offsetRetentionMs
              case Some(expireTimestamp) =>
                // older versions with explicit expire_timestamp field => old expiration semantics is used
                currentTimestamp >= expireTimestamp
            }
          }
      }.map {
        // 为满足以上 3 个条件的分区提取出 commitRecordMetadataAndOffset 中的位移值
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }.toMap
    }

    // 内部调用 getExpiredOffsets 方法获取主题分区的过期位移
    val expiredOffsets: Map[TopicPartition, OffsetAndMetadata] = protocolType match {
      // 如果消费者组状态是 Empty，就传入消费者组变更为 Empty 状态的时间，如果没有该时间则使用提交位移消息本身的写入时间戳来获取过期位移
      case Some(_) if is(Empty) =>
        // no consumer exists in the group =>
        // - if current state timestamp exists and retention period has passed since group became Empty,
        //   expire all offsets with no pending offset commit;
        // - if there is no current state timestamp (old group metadata schema) and retention period has passed
        //   since the last commit timestamp, expire the offset
        getExpiredOffsets(
          commitRecordMetadataAndOffset => currentStateTimestamp
            .getOrElse(commitRecordMetadataAndOffset.offsetAndMetadata.commitTimestamp)
        )

      // 如果是普通的消费者组类型且订阅主题信息已知，就传入提交位移消息本身的写入时间戳和订阅主题集合一起来获取过期位移
      case Some(ConsumerProtocol.PROTOCOL_TYPE) if subscribedTopics.isDefined =>
        // consumers exist in the group =>
        // - if the group is aware of the subscribed topics and retention period had passed since the
        //   the last commit timestamp, expire the offset. offset with pending offset commit are not
        //   expired
        getExpiredOffsets(
          _.offsetAndMetadata.commitTimestamp,
          subscribedTopics.get
        )

      // 如果 protocolType 为 None，表示该消费者组是一个 Standalone 消费者，还是要传入提交位移消息本身的写入时间戳来获取过期位移
      case None =>
        // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage only
        // expire offsets with no pending offset commit that retention period has passed since their last commit
        getExpiredOffsets(_.offsetAndMetadata.commitTimestamp)

      // 如果消费者组的状态不符合上面的这几个 Case，此时不需要被移除
      case _ =>
        Map()
    }

    if (expiredOffsets.nonEmpty)
      debug(s"Expired offsets from group '$groupId': ${expiredOffsets.keySet}")
    // 如果 expiredOffsets 不为空，则直接将过期位移对应的主题分区从 offsets 中移除
    offsets --= expiredOffsets.keySet
    // 返回主题分区对应的过期位移
    expiredOffsets
  }

  def allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[group] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets = offsets.size

  def hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  private def assertValidTransition(targetState: GroupState): Unit = {
    if (!targetState.validPreviousStates.contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, targetState.validPreviousStates.mkString(","), targetState, state))
  }

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}

