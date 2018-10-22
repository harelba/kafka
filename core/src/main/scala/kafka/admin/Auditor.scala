package kafka.admin

import java.util
import java.util.Properties

import kafka.admin.AuditMessageType.AuditMessageType
import kafka.utils.Json
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

class AuditorInitializationException(reason:String) extends Exception(reason)

trait Auditor {
  def initialize(config: Map[String,String]) = {}

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

class KafkaTopicAuditor extends Auditor {
  private var topic:String = null

  @transient var producer:KafkaProducer[String,String] = null

  override def initialize(config: Map[String,String]) = {
    val bootstrapServer = config.get("bootstrap-server") match {
      case Some(s) => s
      case None => throw new AuditorInitializationException("audit target bootstrap-server must be provided")
    }
    topic = config.get("topic") match {
      case Some(t) => t
      case None => throw new AuditorInitializationException("audit target topic must be provided")
    }
    try {
      val p = new Properties
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
      producer = new KafkaProducer[String, String](p)
    }
    catch {
      // TODO add e
      case NonFatal(e) => throw new AuditorInitializationException("Could not initialize audit kafka topic producer")
    }
  }

  override protected def sendAuditInfo(s: String): Unit = {
    val pr = new ProducerRecord[String,String](topic,s)
    producer.send(pr)
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
