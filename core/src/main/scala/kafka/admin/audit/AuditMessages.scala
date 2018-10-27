/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.admin.audit

import java.nio.ByteBuffer
import java.util.Base64

import kafka.admin.audit.AuditEventTimestampSource.AuditEventTimestampSource
import org.apache.kafka.common.record.TimestampType

import scala.collection.JavaConverters._

object AuditMessageType extends Enumeration {
  type AuditMessageType = Value
  val GroupMetadata, GroupOffsets, PartitionAssignment, OffsetCommit, OffsetCommitSnapshot, LogEndOffsetSnapshot,
  AuditPartitionRevoked, AuditPartitionAssigned = Value

}

object AuditEventTimestampSource extends Enumeration {
  type AuditEventTimestampSource = Value
  val CreateTimestamp, LogAppendTimestamp, CommitTimestamp, AuditProcessingTime, GroupMetadataTimestamp, SnapshotReportingTimestamp, RecordTimestamp = Value
}

case class AuditEventTimestampInfo(eventTimestamp: Long,
                                   eventTimestampSource: AuditEventTimestampSource) {
  val asJavaMap = Map(
    "auditEventTimestamp" -> eventTimestamp,
    "auditEventTimestampType" -> eventTimestampSource.toString).asJava
}

trait AuditMessage {
  val asJavaMap: java.util.Map[String,Any]
  val getKey: String
}

case class GroupMetadataAuditMessage(eventTimestampInfo: AuditEventTimestampInfo,
                                     kafkaClusterId:String,
                                     originalKeyTimestamp: Long,
                                     originalKeyTimestampType: TimestampType,
                                     groupId: String,
                                     generationId: Int,
                                     protocolType: Option[String],
                                     currentStateName: String,
                                     currentStateTimestamp: Option[Long],
                                     canRebalance: Boolean) extends AuditMessage {

  private val recordTimestampInfo = Map(
    "originalKeyTimestamp" -> originalKeyTimestamp,
    "originalKeyTimestampType" -> originalKeyTimestampType
  )

  val asJavaMap = (Map(
    "kafkaClusterId" -> kafkaClusterId,
    "groupId" -> groupId,
    "generation" -> generationId,
    "protocolType" -> protocolType.getOrElse(null),
    "currentState" -> currentStateName,
    "currentStateTimestamp" -> currentStateTimestamp.getOrElse(null),
    "canRebalance" -> canRebalance
  ) ++ eventTimestampInfo.asJavaMap.asScala ++ recordTimestampInfo).asJava

  val getKey = kafkaClusterId + groupId
}

case class GroupMetadataOffsetInfoAuditMessage(groupMetadataAuditMessage: GroupMetadataAuditMessage,
                                               topic: String,
                                               partition: Int,
                                               offset: Long,
                                               metadata: String,
                                               commitTimestamp: Long,
                                               expireTimestamp: Option[Long]) extends AuditMessage {

  val asJavaMap = (groupMetadataAuditMessage.asJavaMap.asScala ++ Map(
    "topic" -> topic,
    "partition" -> partition,
    "offset" -> offset,
    "metadata" -> metadata,
    "commitTimestamp" -> commitTimestamp,
    "expireTimestamp" -> expireTimestamp.getOrElse(null)
  )).asJava

  val getKey = groupMetadataAuditMessage.getKey + topic + partition
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
                                                  ) extends AuditMessage {
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

  val getKey = groupMetadataAuditMessage.getKey + topic + partition
}

case class OffsetCommitAuditMessage(eventTimestampInfo: AuditEventTimestampInfo,
                                    kafkaClusterId: String,
                                    groupId: String,
                                    topic: String,
                                    partition: Int,
                                    offset: Long,
                                    leaderEpoch: Option[Integer],
                                    metadata: String,
                                    commitTimestamp: Long,
                                    expireTimestamp: Option[Long]) extends AuditMessage {
  val asJavaMap = (eventTimestampInfo.asJavaMap.asScala ++ Map(
    "kafkaClusterId" -> kafkaClusterId,
    "groupId" -> groupId,
    "topic" -> topic,
    "partition" -> partition,
    "offset" -> offset,
    "leaderEpoch" -> leaderEpoch.orElse(null),
    "metadata" -> metadata,
    "commitTimestamp" -> commitTimestamp,
    "expireTimestamp" -> expireTimestamp.getOrElse(null))).asJava

  val getKey = kafkaClusterId + groupId + topic + partition
}

case class AuditPartitionsRevokedAuditMessage(eventTimestampInfo: AuditEventTimestampInfo,
                                              kafkaClusterId:String,
                                              auditConsumerHost: String,
                                              topic: String,
                                              partition: Int) extends AuditMessage {
  val asJavaMap = (eventTimestampInfo.asJavaMap.asScala ++ Map(
    "kafkaClusterId" -> kafkaClusterId,
    "auditConsumerHost" -> auditConsumerHost,
    "topic" -> topic,
    "partition" -> partition
  )).asJava

  val getKey = kafkaClusterId + topic + partition
}

case class AuditPartitionsAssignedAuditMessage(eventTimestampInfo: AuditEventTimestampInfo,
                                               kafkaClusterId:String,
                                               auditConsumerHost: String,
                                               topic: String,
                                               partition: Int) extends AuditMessage {
  val asJavaMap = (eventTimestampInfo.asJavaMap.asScala ++ Map(
    "kafkaClusterId" -> kafkaClusterId,
    "auditConsumerHost" -> auditConsumerHost,
    "topic" -> topic,
    "partition" -> partition
  )).asJava

  val getKey = kafkaClusterId + topic + partition
}

case class OffsetCommitSnapshot(eventTimestampInfo: Option[AuditEventTimestampInfo],
                                kafkaClusterId: String,
                                groupId: String,
                                topic: String,
                                partition: Int,
                                committedOffset: Long,
                                commitTimestamp: Long,
                                reporterType: String,
                                reportingHost: String,
                                metadata: Map[String, String]) extends AuditMessage {

  private val eventTimestampInfoMap = eventTimestampInfo match {
    case Some(eti) => eti.asJavaMap.asScala
    case None => Map()
  }
  val asJavaMap = (eventTimestampInfoMap ++ Map(
    "kafkaClusterId" -> kafkaClusterId,
    "groupId" -> groupId,
    "topic" -> topic,
    "partition" -> partition,
    "committedOffset" -> committedOffset,
    "commitTimestamp" -> commitTimestamp,
    "reporterType" -> reporterType,
    "reportingHost" -> reportingHost,
    "metadata" -> metadata.asJava
  )).asJava

  val getKey = kafkaClusterId + groupId + topic + partition
}

case class LogEndOffsetSnapshot(eventTimestampInfo: AuditEventTimestampInfo,
                                kafkaClusterId: String,
                                topic: String,
                                partition: Int,
                                logEndOffset: Long,
                                leaderEpoch: Option[Int],
                                reportingHost: String,
                                metadata: Map[String,String]) extends AuditMessage {

  val asJavaMap = (eventTimestampInfo.asJavaMap.asScala ++ Map(
    "kafkaClusterId" -> kafkaClusterId,
    "topic" -> topic,
    "partition" -> partition,
    "logEndOffset" -> logEndOffset,
    "leaderEpoch" -> leaderEpoch.getOrElse(null),
    "reportingHost" -> reportingHost,
    "metadata" -> metadata.asJava)).asJava

  val getKey = kafkaClusterId + topic + partition
}
