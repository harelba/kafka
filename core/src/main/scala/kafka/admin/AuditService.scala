package kafka.admin

import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

import joptsimple.ValueConverter
import kafka.admin.AuditType.AuditType
import kafka.common.OffsetAndMetadata
import kafka.coordinator.group.{GroupMetadataKey, GroupMetadataManager, OffsetKey}
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
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

class AuditService(auditTypes: List[AuditType], bootstrapServer: String, resolveClientHostIps:Boolean,time: Time) extends Logging {
  private val auditConsumer = createAuditConsumer(bootstrapServer)

  private val auditShutdown = new AtomicBoolean(false)
  private val shutdownLatch: CountDownLatch = new CountDownLatch(1)

  def run(auditor: Auditor) = {
    info(s"Starting audit service. Auditor in use is $auditor")

    addAuditShutdownHook()

    try {
      info(s"Audit service subscribing to topic ${Topic.GROUP_METADATA_TOPIC_NAME}")
      auditConsumer.subscribe(List(Topic.GROUP_METADATA_TOPIC_NAME).asJavaCollection, new AuditRebalanceListener(auditor))
      while (!auditShutdown.get()) {
        val records = auditConsumer.poll(Duration.of(Long.MaxValue, ChronoUnit.MILLIS)).asScala
        for (r <- records) {
          try {
            handleRecordAudit(auditor,r)
          }
          catch {
            case NonFatal(e) => {
              error("Could not audit record. Skipping", e)
            }
          }
        }
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
      info("Closing audit consumer")
      auditConsumer.close()
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
              groupTopicPartition.group,
              groupTopicPartition.topicPartition.topic(),
              groupTopicPartition.topicPartition.partition(),
              offsetMessage.offset,
              Option(offsetMessage.leaderEpoch.orElse(null)),
              offsetMessage.metadata,
              offsetMessage.commitTimestamp,
              offsetMessage.expireTimestamp
            )
            auditor.audit(AuditMessageType.OffsetCommit, o.asJavaMap)
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
              r.timestamp(),
              r.timestampType(),
              groupId,
              groupMetadata.generationId,
              groupMetadata.protocolType,
              groupMetadata.currentState.getClass.getSimpleName,
              groupMetadata.currentStateTimestamp,
              groupMetadata.canRebalance
            )

            auditor.audit(AuditMessageType.GroupMetadata, groupMetadataAuditMessage.asJavaMap)

            for ((topicPartition, offsetAndMetadata) <- groupMetadata.allOffsets) {
              val o = GroupMetadataOffsetInfoAuditMessage(groupMetadataAuditMessage,
                topicPartition.topic(),
                topicPartition.partition(),
                offsetAndMetadata.offset,
                offsetAndMetadata.metadata,
                offsetAndMetadata.commitTimestamp,
                offsetAndMetadata.expireTimestamp)

              auditor.audit(AuditMessageType.GroupOffsets, o.asJavaMap)
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

                auditor.audit(AuditMessageType.PartitionAssignment, o.asJavaMap)
              }
            }
          case None => //
        }
      case _ => // separately parse offset commits
    }
  }

  private def resolveClientHostIfNeeded(clientHost: String) : String = {
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
        warn(s"Could not resolve client host ${clientHost}. Leaving it as it is",e)
        clientHost
      }
    }
  }

  private def createAuditConsumer(bootstrapServer: String) = {
    val properties = new Properties()
    val deserializer = (new ByteArrayDeserializer).getClass.getName
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "__audit-consumer-group-1")
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

class AuditRebalanceListener(auditor: Auditor) extends ConsumerRebalanceListener {
  private val auditConsumerHost = InetAddress.getLocalHost.getCanonicalHostName

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.asScala.foreach { tp =>
      val o = AuditPartitionsRevokedAuditMessage(AuditEventTimestampInfo(System.currentTimeMillis(),AuditEventTimestampSource.AuditProcessingTime), auditConsumerHost, tp.topic(), tp.partition())
      auditor.audit(AuditMessageType.AuditPartitionRevoked, o.asJavaMap)
    }
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.asScala.foreach { tp =>
      val o = AuditPartitionsAssignedAuditMessage(AuditEventTimestampInfo(System.currentTimeMillis(),AuditEventTimestampSource.AuditProcessingTime), auditConsumerHost, tp.topic(), tp.partition())
      auditor.audit(AuditMessageType.AuditPartitionAssigned, o.asJavaMap)
    }
  }
}

object AuditType extends Enumeration {
  type AuditType = Value
  val GroupMetadata, OffsetCommits = Value

  def valueConverter = new ValueConverter[AuditType] {
    override def valueType(): Class[_ <: AuditType] = classOf[AuditType]
    override def convert(value: String): AuditType = AuditType.withName(value)
    override def valuePattern(): String = AuditType.values mkString ","
  }
}

object AuditTarget extends Enumeration {
  type AuditTarget = Value
  val StdOut,LogFile = Value

  def valueConverter = new ValueConverter[AuditTarget] {
    override def valueType(): Class[_ <: AuditTarget] = classOf[AuditTarget]
    override def convert(value: String): AuditTarget = AuditTarget.withName(value)
    override def valuePattern(): String = AuditTarget.values mkString ","
  }
}


