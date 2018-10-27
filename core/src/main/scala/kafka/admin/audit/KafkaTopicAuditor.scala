package kafka.admin.audit

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.control.NonFatal

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
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      producer = new KafkaProducer[String, String](p)
    }
    catch {
      // TODO add e
      case NonFatal(e) => throw new AuditorInitializationException("Could not initialize audit kafka topic producer",e)
    }
  }

  override protected def sendAuditInfo(key:String,s: String): Unit = {
    val pr = new ProducerRecord[String,String](topic,key,s)
    producer.send(pr)
  }
}
