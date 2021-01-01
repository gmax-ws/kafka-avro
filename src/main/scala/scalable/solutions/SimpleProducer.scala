package scalable.solutions

import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import scalable.solutions.KafkaUtils._
import scalable.solutions.message.EventMessage

import java.util.{Date, Properties, Random, UUID}
import scala.util.Using

object SimpleProducer extends App {

  private val logger = LoggerFactory.getLogger("SimpleProducer")

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip")
  props.put(ProducerConfig.ACKS_CONFIG, "1")
  props.put(ProducerConfig.RETRIES_CONFIG, 0)
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
  props.put(ProducerConfig.LINGER_MS_CONFIG, 1)
  props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
  props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl(USE_DNS))

  val machines = Array("pump_1", "pump_2", "tank_1", "tank_2")
  val minX = 1f
  val maxX = 100.0f
  val rand = new Random()

  Using(new KafkaProducer[String, EventMessage](props)) { producer =>
    val event = new EventMessage()
    event.setBuilding("building_3")

    (1 to 10).foreach { _ =>
      val id = UUID.randomUUID().toString
      val status = rand.nextFloat() * (maxX - minX) + minX
      event.setId(id)
      event.setStatus(status.toString)
      event.setMachine(machines(rand.nextInt(machines.length)))
      event.setDate(new Date().getTime)

      val record = new ProducerRecord[String, EventMessage](TOPIC_NAME, id, event)

      val m = producer.send(record).get()
      logger.info(s"Message produced, id: $id offset: ${m.offset} partition: ${m.partition} topic: ${m.topic} timestamp: ${m.timestamp}")
    }
  }.toEither match {
    case Right(_) => logger.info("Producer success.")
    case Left(e) => logger.error("Producer error.", e)
  }
}
