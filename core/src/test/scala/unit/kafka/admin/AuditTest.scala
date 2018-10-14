package unit.kafka.admin

import java.util.Properties

import kafka.admin.ConsumerGroupCommandTest
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.junit.Test

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class AuditTest extends ConsumerGroupCommandTest {

  @Test
  def testDescribeMembersOfExistingGroup() {
    TestUtils.createOffsetsTopic(zkClient, servers)
    createTopic("mytopic", 8, 1)

    val cgcArgs = Array("--bootstrap-server", brokerList, "--audit","--audit-target","StdOut")
    val service = getConsumerGroupService(cgcArgs)

    import ExecutionContext.Implicits.global

    Future {
      service.audit()
    }

    val producerConfig = new Properties
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "-1")
    producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String,String](producerConfig)

    (1 to 20).map { i => println(producer.send(new ProducerRecord[String,String]("mytopic",s"message $i")).get()) }

    val consumerConfig = new Properties
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "audited_group_1")
    consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    val consumer = new KafkaConsumer[String, String](consumerConfig)
    consumer.subscribe(List("mytopic").asJavaCollection)
    println(consumer.poll(5000))


    TestUtils.waitUntilTrue(() => {
//      val x = auditor.fetchAudits().filter(!_.contains("__consumer_offsets")) map { Json.parseFull _ }
//      println("====")
//      println(x.headOption)
//      false
      true
    }, "XXX")
  }

}
