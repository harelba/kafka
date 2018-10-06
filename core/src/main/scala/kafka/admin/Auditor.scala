package kafka.admin

import java.util

import kafka.admin.AuditMessageType.AuditMessageType
import kafka.utils.Json
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

class Auditor {
  private lazy val auditLogger = LoggerFactory.getLogger("consumer_groups_audit_logger")

  def sendAuditInfo(messageType: AuditMessageType, javaMap: util.Map[String, Any]): Unit = {
    val s = Json.encodeAsString((javaMap.asScala ++ Map("kafkaAuditMessageType" -> messageType.toString)).asJava)
    auditLogger.info(s)
  }
}
