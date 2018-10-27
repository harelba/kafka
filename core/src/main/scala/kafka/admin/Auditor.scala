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

import java.util
import java.util.Properties

import kafka.admin.AuditMessageType.AuditMessageType
import kafka.utils.Json
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

class AuditorInitializationException(reason:String,e:Throwable) extends Exception(reason,e) {
  def this(reason:String) = this(reason,null)
}

trait Auditor {
  def initialize(config: Map[String,String]) = {}

  def audit(messageType: AuditMessageType, javaMap: util.Map[String, Any],key:String) = {
    val s = Json.encodeAsString((javaMap.asScala ++ Map("kafkaAuditMessageType" -> messageType.toString)).asJava)
    sendAuditInfo(key,s)
  }

  protected def sendAuditInfo(key:String,s: String)
}

class Log4JAuditor extends Auditor {
  private lazy val auditLogger = LoggerFactory.getLogger("consumer_groups_audit_logger")

  def sendAuditInfo(key:String,s: String) = {
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

class StdOutAuditor extends Auditor {
  def sendAuditInfo(key:String,s: String) = {
    println(s)
  }
}

class InMemoryAuditor extends Auditor {
  private val audits = mutable.Buffer[(String,String)]()

  override def sendAuditInfo(key:String,s: String) = {
    val t = (key,s)
    audits += t
  }

  def fetchAudits() = audits.toList

  def clearAudits() = audits.clear()
}
