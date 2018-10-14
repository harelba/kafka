package kafka.admin

import java.util

import kafka.admin.AuditMessageType.AuditMessageType
import kafka.utils.Json
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

trait Auditor {
  def audit(messageType: AuditMessageType, javaMap: util.Map[String, Any]) = {
    val s = Json.encodeAsString((javaMap.asScala ++ Map("kafkaAuditMessageType" -> messageType.toString)).asJava)
    sendAuditInfo(s)
  }

  protected def sendAuditInfo(s: String)
}

class Log4JAuditor extends Auditor {
  private lazy val auditLogger = LoggerFactory.getLogger("consumer_groups_audit_logger")

  def sendAuditInfo(s: String) = {
    auditLogger.info(s)
  }
}

class StdOutAuditor extends Auditor {
  def sendAuditInfo(s: String) = {
    println(s)
  }
}

class InMemoryAuditor extends Auditor {
  private val audits = mutable.Buffer[String]()

  override def sendAuditInfo(s: String) = {
    audits += s
  }

  def fetchAudits() = audits.toList

  def clearAudits() = audits.clear()
}
