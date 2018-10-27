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

package kafka.admin

import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch}

import joptsimple.ValueConverter
import kafka.admin.AuditType.AuditType
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.{GroupMetadataKey, GroupMetadataManager, GroupTopicPartition, OffsetKey}
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class AuditService(auditTypes: List[AuditType],
                   bootstrapServer: String,
                   resolveClientHostIps: Boolean,
                   kafkaClusterId: String,
                   auditConsumerGroupId: String,
                   offsetCommitSnapshotSendIntervalMs: Int,
                   offsetCommitSnapshotCleanupIntervalMs: Int,
                   time: Time) extends Logging {

  private val auditConsumer = createConsumer(bootstrapServer, auditConsumerGroupId)

  private val auditShutdown = new AtomicBoolean(false)
  private val shutdownLatch: CountDownLatch = new CountDownLatch(1)

  private val hostname = InetAddress.getLocalHost.getCanonicalHostName

  private def shouldTrackOffsetCommits() = {
    auditTypes contains AuditType.OffsetCommitSnapshots
  }

  def run(auditor: Auditor) = {
    info(s"Starting audit service on host $hostname. Auditor in use is $auditor")

    addAuditShutdownHook()

    try {
      info(s"Audit service subscribing to topic ${Topic.GROUP_METADATA_TOPIC_NAME}")
      auditConsumer.subscribe(List(Topic.GROUP_METADATA_TOPIC_NAME).asJavaCollection, new AuditRebalanceListener(auditor, kafkaClusterId))
      while (!auditShutdown.get()) {
        val records = auditConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).asScala
        for (r <- records) {
          try {
            handleRecordAudit(auditor, r)
            trackOffsetCommit(auditor, r)
          }
          catch {
            case NonFatal(e) => {
              error("Could not audit record. Skipping", e)
            }
          }
        }
        sendOffsetCommitSnapshotsIfNeeded(auditor)
      }
      info("Audit consumer loop has ended")
    }
    catch {
      case e: WakeupException =>
        info("Audit service woken up")
        if (!auditShutdown.get()) {
          error("Waking up was not part of a shutdown", e)
          throw e
        }
    }
    finally {
      val closeTimeout = 5.second
      info(s"Closing audit consumer. Closing timeout is $closeTimeout")
      try {
        val closeConsumerFuture = Future {
          auditConsumer.close()
        }
        Await.result(closeConsumerFuture, closeTimeout)
      }
      catch {
        case e: TimeoutException => error("Could not close audit consumer within a reasonable timeout", e)
        case NonFatal(e1) => error("Error while trying to close audit consumer", e1)
      }
      info("Signalling that audit consumer loop has been shut down")
      shutdownLatch.countDown()
      info("Signalled that audit consumer loop has been shut down.")
    }
  }

  private def handleRecordAudit(auditor: Auditor, r: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    for (auditType <- auditTypes) {
      auditType match {
        case AuditType.GroupMetadata => auditGroupMetadataRecord(auditor, r)
        case AuditType.OffsetCommits => auditOffsetCommitRecord(auditor, r)
        case _ => // (AuditType.OffsetCommitSnapshots is handled independently, since it's periodic
      }
    }
  }

  private def calculateNextOffsetCommitSnapshotTime(refTime: Long) = {
    (refTime / offsetCommitSnapshotSendIntervalMs) * offsetCommitSnapshotSendIntervalMs + offsetCommitSnapshotSendIntervalMs
  }

  case class OffsetCommitBreadcrumb(commitOffset: Long, commitTimestamp: Long, lastUpdatedAt: Long)


  val offsetBreadcrumbs: ConcurrentHashMap[GroupTopicPartition, OffsetCommitBreadcrumb] = new ConcurrentHashMap
  var nextOffsetCommitSnapshotsTime: Long = calculateNextOffsetCommitSnapshotTime(time.milliseconds())

  private def shouldSendOffsetCommitSnapshots(refTime: Long): Boolean = {
    val b = refTime > nextOffsetCommitSnapshotsTime
    if (isDebugEnabled)
      debug(s"refTime=$refTime nextOffsetCommitSnapshotsTime=$nextOffsetCommitSnapshotsTime delta ${nextOffsetCommitSnapshotsTime - refTime} b=$b")
    b
  }

  private def cleanOffsetCommitSnapshots(refTime: Long) = {
    val tooOldThreshold = refTime - offsetCommitSnapshotCleanupIntervalMs
    val forDeletion = offsetBreadcrumbs.asScala
      .filter { case (k, v) => v.lastUpdatedAt < tooOldThreshold }

    forDeletion.foreach { case (k, v) =>
      info(s"Cleaning old offset commit snapshots for group/topic/partition $k - Last breadcrumb is $v. Time last seen is ${refTime - v.lastUpdatedAt} ms")
      offsetBreadcrumbs.remove(k)
    }
  }

  private def sendOffsetCommitSnapshotsIfNeeded(auditor: Auditor) = {
    val snapshotTimestamp = time.milliseconds()
    if (shouldSendOffsetCommitSnapshots(snapshotTimestamp)) {
      nextOffsetCommitSnapshotsTime = calculateNextOffsetCommitSnapshotTime(snapshotTimestamp)

      cleanOffsetCommitSnapshots(snapshotTimestamp)

      info("Sending offset commit snapshots")

      val snapshots = for ((groupTopicPartition, breadcrumb) <- offsetBreadcrumbs.asScala)
        yield {
          val snapshotTimestampInfo = AuditEventTimestampInfo(breadcrumb.commitTimestamp, AuditEventTimestampSource.CommitTimestamp)
          OffsetCommitSnapshot(Some(snapshotTimestampInfo),
            kafkaClusterId,
            groupTopicPartition.group,
            groupTopicPartition.topicPartition.topic(),
            groupTopicPartition.topicPartition.partition(),
            breadcrumb.commitOffset,
            breadcrumb.commitTimestamp,
            "kafka-auditor",
            hostname,
            Map())

        }

      snapshots.foreach { snapshot =>
        auditor.audit(AuditMessageType.OffsetCommitSnapshot, snapshot.asJavaMap, snapshot.getKey)
      }
      info("Sent offset commit snapshots")

      val topicPartitions = snapshots.map({ x =>
        new TopicPartition(x.topic, x.partition) -> x.commitTimestamp
      }
      ).toMap

      info("Retrieving logEndOffset snapshots")

      val logEndOffsetMessages = if (isKafka10) {
        retrieveKafka10BasedLogEndOffsets(topicPartitions)
      }
      else {
        retrieveKafka8BasedLogEndOffsets(snapshotTimestamp, topicPartitions)
      }
      info(s"Sending ${logEndOffsetMessages.size} log end offset snapshots")
      for (m <- logEndOffsetMessages) {
        auditor.audit(AuditMessageType.LogEndOffsetSnapshot, m.asJavaMap, m.getKey)
      }
    }
  }

  private def retrieveKafka8BasedLogEndOffsets(snapshotTimestamp: Long, topicPartitions: Map[TopicPartition, Long]) = {
    val latestOffsetsForTopicPartitions = auditConsumer.endOffsets(topicPartitions.keys.asJavaCollection).asScala
    topicPartitions.keys.map { k =>
      (k, latestOffsetsForTopicPartitions.get(k)) match {
        case (tp, Some(o)) => Option((tp, o))
        case (tp, None) => None
      }
    } collect {
      case Some((tp, o)) => {
        val timestampInfo = AuditEventTimestampInfo(snapshotTimestamp, AuditEventTimestampSource.SnapshotReportingTimestamp)
        LogEndOffsetSnapshot(timestampInfo, kafkaClusterId, tp.topic(), tp.partition(), o, None, hostname, Map())
      }
    }
  }

  private def retrieveKafka10BasedLogEndOffsets(topicPartitions: Map[TopicPartition, Long]) = {
    val javaBasedTopicPartitions = topicPartitions.map { case (k, v) => (k, new java.lang.Long(v)) }.asJava
    val offsetsForSnapshotTime = auditConsumer.offsetsForTimes(javaBasedTopicPartitions).asScala
    offsetsForSnapshotTime.collect {
      case (tp, oat) if oat != null =>
        val timestampInfo = AuditEventTimestampInfo(oat.timestamp(), AuditEventTimestampSource.RecordTimestamp)

        val leaderEpoch: Option[Int] = if (oat.leaderEpoch().isPresent) Option(oat.leaderEpoch().get()) else Option(null.asInstanceOf[Int])
        LogEndOffsetSnapshot(timestampInfo, kafkaClusterId, tp.topic(), tp.partition(), oat.offset(), leaderEpoch, hostname, Map())
      // TODO Should we send log-end-offsets for cases where oat is null? Perhaps no for kafka10
    }
  }

  def isKafka10 = {
    false
  }

  private def trackOffsetCommit(auditor: Auditor, r: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    if (shouldTrackOffsetCommits()) {
      Option(r.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        case offsetKey: OffsetKey =>
          val groupTopicPartition = offsetKey.key
          val value = r.value
          Option(value) match {
            case Some(v) =>
              val offsetMessage = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value))

              val b = OffsetCommitBreadcrumb(offsetMessage.offset, offsetMessage.commitTimestamp, time.milliseconds())

              val lastBreadcrumbOpt = Option(offsetBreadcrumbs.get(groupTopicPartition))
              lastBreadcrumbOpt match {
                case None =>
                  offsetBreadcrumbs.put(groupTopicPartition, b)
                case Some(lastBreadcrumb) if lastBreadcrumb.commitOffset != b.commitOffset =>
                  offsetBreadcrumbs.put(groupTopicPartition, b)
                case Some(lastBreadcrumb) if lastBreadcrumb.commitOffset == b.commitOffset => //
              }
            case None => //
          }
        case _ => // no-op
      }
    }
  }

  private def auditOffsetCommitRecord(auditor: Auditor, r: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    Option(r.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
      case offsetKey: OffsetKey =>
        val groupTopicPartition = offsetKey.key
        val value = r.value
        Option(value) match {
          case Some(v) =>
            val offsetMessage = GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value))

            val eventTimestampInfo = calculateOffsetCommitEventTimestampInfo(offsetMessage)

            val o = OffsetCommitAuditMessage(
              eventTimestampInfo,
              kafkaClusterId,
              groupTopicPartition.group,
              groupTopicPartition.topicPartition.topic(),
              groupTopicPartition.topicPartition.partition(),
              offsetMessage.offset,
              Option(offsetMessage.leaderEpoch.orElse(null)),
              offsetMessage.metadata,
              offsetMessage.commitTimestamp,
              offsetMessage.expireTimestamp
            )
            auditor.audit(AuditMessageType.OffsetCommit, o.asJavaMap, o.getKey)
          case None => //
        }
      case _ => // no-op
    }
  }

  private def auditGroupMetadataRecord(auditor: Auditor, r: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    Option(r.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
      case groupMetadataKey: GroupMetadataKey =>
        val groupId = groupMetadataKey.key
        val value = r.value
        Option(value) match {
          case Some(v) =>
            val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(r.value), time)

            val eventTimestampInfo = calculateGroupMetadataEventTimestampInfo(r, groupMetadata.currentStateTimestamp)

            val groupMetadataAuditMessage = GroupMetadataAuditMessage(
              eventTimestampInfo,
              kafkaClusterId,
              r.timestamp(),
              r.timestampType(),
              groupId,
              groupMetadata.generationId,
              groupMetadata.protocolType,
              groupMetadata.currentState.getClass.getSimpleName,
              groupMetadata.currentStateTimestamp,
              groupMetadata.canRebalance
            )

            auditor.audit(AuditMessageType.GroupMetadata, groupMetadataAuditMessage.asJavaMap, groupMetadataAuditMessage.getKey)

            for ((topicPartition, offsetAndMetadata) <- groupMetadata.allOffsets) {
              val o = GroupMetadataOffsetInfoAuditMessage(groupMetadataAuditMessage,
                topicPartition.topic(),
                topicPartition.partition(),
                offsetAndMetadata.offset,
                offsetAndMetadata.metadata,
                offsetAndMetadata.commitTimestamp,
                offsetAndMetadata.expireTimestamp)

              auditor.audit(AuditMessageType.GroupOffsets, o.asJavaMap, o.getKey)
            }

            for (memberMetadata <- groupMetadata.allMemberMetadata) {
              val assignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(memberMetadata.assignment))

              for (pt <- assignment.partitions().asScala) {
                val o = GroupMetadataMemberMetadataAuditMessage(groupMetadataAuditMessage,
                  memberMetadata.memberId,
                  memberMetadata.clientId,
                  resolveClientHostIfNeeded(memberMetadata.clientHost),
                  memberMetadata.rebalanceTimeoutMs,
                  memberMetadata.sessionTimeoutMs,
                  memberMetadata.protocolType,
                  memberMetadata.supportedProtocols,
                  pt.topic(),
                  pt.partition(),
                  assignment.userData())

                auditor.audit(AuditMessageType.PartitionAssignment, o.asJavaMap, o.getKey)
              }
            }
          case None => //
        }
      case _ => // separately parse offset commits
    }
  }

  private def resolveClientHostIfNeeded(clientHost: String): String = {
    try {
      val cleanClientHost = if (clientHost.startsWith("/")) clientHost.substring(1) else clientHost
      if (resolveClientHostIps) {
        Try(InetAddress.getByName(cleanClientHost)) match {
          case Success(a) => a.getHostName
          case Failure(e) => cleanClientHost
        }
      }
      else {
        cleanClientHost
      }
    }
    catch {
      case NonFatal(e) => {
        warn(s"Could not resolve client host $clientHost. Leaving it as it is", e)
        clientHost
      }
    }
  }

  private def createConsumer(bootstrapServer: String, auditConsumerGroupId: String) = {
    val properties = new Properties()
    val deserializer = (new ByteArrayDeserializer).getClass.getName
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, auditConsumerGroupId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
    new KafkaConsumer[Array[Byte], Array[Byte]](properties)
  }

  private def addAuditShutdownHook() = {
    Runtime.getRuntime.addShutdownHook(new Thread("kafkaAuditShutdownThread") {
      override def run() {
        info("System is shutting down. Waiting for audit consumer to shut down.")
        auditShutdown.set(true)
        auditConsumer.wakeup()
        try {
          shutdownLatch.await()
          info("Audit consumer has been shut down")
        } catch {
          case _: InterruptedException =>
            warn("Audit consumer shutdown has been interrupted")
        }

      }
    })
  }

  private def calculateOffsetCommitEventTimestampInfo(offsetMessage: OffsetAndMetadata) = {
    AuditEventTimestampInfo(offsetMessage.commitTimestamp, AuditEventTimestampSource.CommitTimestamp)
  }

  private def calculateGroupMetadataEventTimestampInfo(r: ConsumerRecord[Array[Byte], Array[Byte]],
                                                       currentStateTimestamp: Option[Long]): AuditEventTimestampInfo = {
    currentStateTimestamp match {
      case Some(ts) =>
        AuditEventTimestampInfo(ts, AuditEventTimestampSource.GroupMetadataTimestamp)
      case None => {
        r.timestampType() match {
          case TimestampType.NO_TIMESTAMP_TYPE =>
            AuditEventTimestampInfo(time.milliseconds(), AuditEventTimestampSource.AuditProcessingTime)
          case TimestampType.CREATE_TIME =>
            AuditEventTimestampInfo(r.timestamp(), AuditEventTimestampSource.CreateTimestamp)
          case TimestampType.LOG_APPEND_TIME =>
            AuditEventTimestampInfo(r.timestamp(), AuditEventTimestampSource.LogAppendTimestamp)
        }
      }
    }

  }
}

class AuditRebalanceListener(auditor: Auditor, kafkaClusterId: String) extends ConsumerRebalanceListener {
  private val auditConsumerHost = InetAddress.getLocalHost.getCanonicalHostName

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.asScala.foreach { tp =>
      val o = AuditPartitionsRevokedAuditMessage(AuditEventTimestampInfo(System.currentTimeMillis(), AuditEventTimestampSource.AuditProcessingTime), kafkaClusterId, auditConsumerHost, tp.topic(), tp.partition())
      auditor.audit(AuditMessageType.AuditPartitionRevoked, o.asJavaMap, o.getKey)
    }
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.asScala.foreach { tp =>
      val o = AuditPartitionsAssignedAuditMessage(AuditEventTimestampInfo(System.currentTimeMillis(), AuditEventTimestampSource.AuditProcessingTime), kafkaClusterId, auditConsumerHost, tp.topic(), tp.partition())
      auditor.audit(AuditMessageType.AuditPartitionAssigned, o.asJavaMap, o.getKey)
    }
  }
}

object AuditType extends Enumeration {
  type AuditType = Value
  val GroupMetadata, OffsetCommits, OffsetCommitSnapshots = Value

  def valueConverter = new ValueConverter[AuditType] {
    override def valueType(): Class[_ <: AuditType] = classOf[AuditType]

    override def convert(value: String): AuditType = AuditType.withName(value)

    override def valuePattern(): String = AuditType.values mkString ","
  }
}

object AuditTarget extends Enumeration {
  type AuditTarget = Value
  val StdOut, LogFile, KafkaTopic = Value

  def valueConverter = new ValueConverter[AuditTarget] {
    override def valueType(): Class[_ <: AuditTarget] = classOf[AuditTarget]

    override def convert(value: String): AuditTarget = AuditTarget.withName(value)

    override def valuePattern(): String = AuditTarget.values mkString ","
  }
}


