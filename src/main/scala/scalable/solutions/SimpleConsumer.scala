package scalable.solutions

import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroDeserializer, KafkaAvroDeserializerConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import scalable.solutions.KafkaUtils._
import scalable.solutions.message.EventMessage

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._
import scala.util.Using

object SimpleConsumer extends App {

  private val logger = LoggerFactory.getLogger("SimpleConsumer")

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
  props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS)
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER)
  props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
  props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true")
  props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl(USE_DNS))

  Using(new KafkaConsumer[String, EventMessage](props)) { consumer =>
    consumer.subscribe(Collections.singletonList(TOPIC_NAME))
    while (true) {
      consumer.poll(Duration.ofMillis(100)).asScala foreach { record =>
        logger.info(s"topic: ${record.topic}, partition: ${record.partition}, offset: ${record.offset}, " +
          s"key: ${record.key}, value: ${record.value}, timestamp: ${record.timestamp}")
      }
    }
  }.toEither match {
    case Right(_) => logger.info("Consumer success.")
    case Left(e) => logger.error("Consumer error.", e)
  }
}
