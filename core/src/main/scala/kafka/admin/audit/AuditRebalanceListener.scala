package kafka.admin.audit


import java.net.InetAddress
import java.util

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

class AuditRebalanceListener(auditor: Auditor, kafkaClusterId: String) extends ConsumerRebalanceListener {
  private val auditConsumerHost = InetAddress.getLocalHost.getCanonicalHostName

  override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.asScala.foreach { tp =>
      val o = AuditPartitionsRevokedAuditMessage(AuditEventTimestampInfo(System.currentTimeMillis(), AuditEventTimestampSource.AuditProcessingTime), kafkaClusterId, auditConsumerHost, tp.topic(), tp.partition())
      auditor.audit(AuditMessageType.AuditPartitionRevoked, o)
    }
  }

  override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.asScala.foreach { tp =>
      val o = AuditPartitionsAssignedAuditMessage(AuditEventTimestampInfo(System.currentTimeMillis(), AuditEventTimestampSource.AuditProcessingTime), kafkaClusterId, auditConsumerHost, tp.topic(), tp.partition())
      auditor.audit(AuditMessageType.AuditPartitionAssigned, o)
    }
  }
}
