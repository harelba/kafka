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

package kafka.admin

import java.nio.ByteBuffer
import java.util.Base64

import kafka.admin.AuditEventTimestampSource.AuditEventTimestampSource
import org.apache.kafka.common.record.TimestampType

import scala.collection.JavaConverters._

object AuditMessageType extends Enumeration {
  type AuditMessageType = Value
  val GroupMetadata, GroupOffsets, PartitionAssignment, OffsetCommit, AuditPartitionRevoked, AuditPartitionAssigned = Value

}

object AuditEventTimestampSource extends Enumeration {
  type AuditEventTimestampSource = Value
  val CreateTimestamp,LogAppendTimestamp,CommitTimestamp,AuditProcessingTime,GroupMetadataTimestamp = Value
}

case class AuditEventTimestampInfo(eventTimestamp: Long,
                                   eventTimestampSource: AuditEventTimestampSource) {
  val asJavaMap = Map(
    "auditEventTimestamp" -> eventTimestamp,
    "auditEventTimestampType" -> eventTimestampSource.toString).asJava
}

case class GroupMetadataAuditMessage(eventTimestampInfo: AuditEventTimestampInfo,
                                     originalKeyTimestamp: Long,
                                     originalKeyTimestampType: TimestampType,
                                     groupId: String,
                                     generationId: Int,
                                     protocolType: Option[String],
                                     currentStateName: String,
                                     currentStateTimestamp: Option[Long],
                                     canRebalance: Boolean) {

  private val recordTimestampInfo = Map(
    "originalKeyTimestamp" -> originalKeyTimestamp,
    "originalKeyTimestampType" -> originalKeyTimestampType
  )

  val asJavaMap = (Map(
    "groupId" -> groupId,
    "generation" -> generationId,
    "protocolType" -> protocolType.getOrElse(null),
    "currentState" -> currentStateName,
    "currentStateTimestamp" -> currentStateTimestamp.getOrElse(null),
    "canRebalance" -> canRebalance
  ) ++ eventTimestampInfo.asJavaMap.asScala ++ recordTimestampInfo).asJava
}

case class GroupMetadataOffsetInfoAuditMessage(groupMetadataAuditMessage: GroupMetadataAuditMessage,
                                               topic: String,
                                               partition: Int,
                                               offset: Long,
                                               metadata: String,
                                               commitTimestamp: Long,
                                               expireTimestamp: Option[Long]) {

  val asJavaMap = (groupMetadataAuditMessage.asJavaMap.asScala ++ Map(
    "topic" -> topic,
    "partition" -> partition,
    "offset" -> offset,
    "metadata" -> metadata,
    "commitTimestamp" -> commitTimestamp,
    "expireTimestamp" -> expireTimestamp.getOrElse(null)
  )).asJava
}

case class GroupMetadataMemberMetadataAuditMessage(groupMetadataAuditMessage: GroupMetadataAuditMessage,
                                                   memberId: String,
                                                   clientId: String,
                                                   clientHost: String,
                                                   rebalanceTimeoutMs: Int,
                                                   sessionTimeoutMs: Int,
                                                   protocolType: String,
                                                   supportedProtocols: List[(String, Array[Byte])],
                                                   topic: String,
                                                   partition: Int,
                                                   userData: ByteBuffer
                                                  ) {
  val asJavaMap = (groupMetadataAuditMessage.asJavaMap.asScala ++ Map(
    "memberId" -> memberId,
    "clientId" -> clientId,
    "clientHost" -> clientHost,
    "rebalanceTimeoutMs" -> rebalanceTimeoutMs,
    "sessionTimeoutMs" -> sessionTimeoutMs,
    "protocolType" -> protocolType,
    "supportedProtocols" -> supportedProtocols.map({
      case (protocol, metadata) => Map("protocol" -> protocol, "metadata" -> Base64.getEncoder.encode(metadata)).asJava
    }).asJava,
    "topic" -> topic,
    "partition" -> partition,
    "userData" -> Base64.getEncoder.encode(userData)
  )).asJava
}

case class OffsetCommitAuditMessage(eventTimestampInfo: AuditEventTimestampInfo,
                                    groupId: String,
                                    topic: String,
                                    partition: Int,
                                    offset: Long,
                                    leaderEpoch: Option[Integer],
                                    metadata: String,
                                    commitTimestamp: Long,
                                    expireTimestamp: Option[Long]) {
  val asJavaMap = (eventTimestampInfo.asJavaMap.asScala ++ Map(
    "group" -> groupId,
    "topic" -> topic,
    "partition" -> partition,
    "offset" -> offset,
    "leaderEpoch" -> leaderEpoch.orElse(null),
    "metadata" -> metadata,
    "commitTimestamp" -> commitTimestamp,
    "expireTimestamp" -> expireTimestamp.getOrElse(null))).asJava
}

case class AuditPartitionsRevokedAuditMessage(eventTimestampInfo: AuditEventTimestampInfo, auditConsumerHost: String, topic: String, partition: Int) {
  val asJavaMap = (eventTimestampInfo.asJavaMap.asScala ++ Map(
    "auditConsumerHost" -> auditConsumerHost,
    "topic" -> topic,
    "partition" -> partition
  )).asJava
}

case class AuditPartitionsAssignedAuditMessage(eventTimestampInfo: AuditEventTimestampInfo, auditConsumerHost: String, topic: String, partition: Int) {
  val asJavaMap = (eventTimestampInfo.asJavaMap.asScala ++ Map(
    "auditConsumerHost" -> auditConsumerHost,
    "topic" -> topic,
    "partition" -> partition
  )).asJava
}

