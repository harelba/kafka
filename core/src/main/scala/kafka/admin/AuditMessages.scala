package kafka.admin

import java.nio.ByteBuffer
import java.util.Base64

import org.apache.kafka.common.record.TimestampType

import scala.collection.JavaConverters._

case class GroupMetadataAuditMessage(originalKeyTimestamp: Long,
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

  private val eventTimestampInfo = currentStateTimestamp match {
    case Some(ts) => Map("@timestamp" -> ts, "kafkaTimestampType" -> "GroupMetadataTime")
    case None => Map("@timestamp" -> originalKeyTimestamp, "kafkaTimestampType" -> originalKeyTimestampType)
  }

  val asJavaMap = (Map(
    "groupId" -> groupId,
    "generation" -> generationId,
    "protocolType" -> protocolType.orNull,
    "currentState" -> currentStateName,
    "currentStateTimestamp" -> currentStateTimestamp.orNull,
    "canRebalance" -> canRebalance
  ) ++ eventTimestampInfo ++ recordTimestampInfo).asJava
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
    "expireTimestamp" -> expireTimestamp.orNull
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
    }),
    "topic" -> topic,
    "partition" -> partition,
    "userData" -> Base64.getEncoder.encode(userData)
  )).asJava
}

case class OffsetCommitAuditMessage(timestamp: Long,
                                    groupId: String,
                                    topic: String,
                                    partition: Int,
                                    offset: Long,
                                    leaderEpoch: Option[Integer],
                                    metadata: String,
                                    commitTimestamp: Long,
                                    expireTimestamp: Option[Long]) {
  val asJavaMap = Map(
    "@timestamp" -> timestamp,
    "group" -> groupId,
    "topic" -> topic,
    "partition" -> partition,
    "offset" -> offset,
    "leaderEpoch" -> leaderEpoch.orElse(null),
    "metadata" -> metadata,
    "commitTimestamp" -> commitTimestamp,
    "expireTimestamp" -> expireTimestamp.orNull).asJava
}

case class AuditPartitionsRevokedAuditMessage(timestamp: Long,auditConsumerHost: String,topic: String,partition:Int) {
  val asJavaMap = Map(
    "@timestamp" -> timestamp,
    "auditConsumerHost" -> auditConsumerHost,
    "topic" -> topic,
    "partition" -> partition
  ).asJava
}

case class AuditPartitionsAssignedAuditMessage(timestamp: Long,auditConsumerHost: String,topic: String,partition:Int) {
  val asJavaMap = Map(
    "@timestamp" -> timestamp,
    "auditConsumerHost" -> auditConsumerHost,
    "topic" -> topic,
    "partition" -> partition
  ).asJava
}
