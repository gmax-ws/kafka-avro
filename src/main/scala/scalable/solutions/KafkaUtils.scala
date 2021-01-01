package scalable.solutions

import com.google.gson.Gson
import org.apache.zookeeper.ZooKeeper
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Using

case class KafkaBrokerInfo(host: String, port: Int)

object KafkaUtils {

  private val logger = LoggerFactory.getLogger("KafkaUtils")

  val CLIENT_ID: String = "client-avro"
  val TOPIC_NAME: String = "events-avro"
  val GROUP_ID_CONFIG: String = "events-avro-group"
  val OFFSET_RESET_EARLIER: String = "earliest"
  val MAX_POLL_RECORDS: Int = 1
  val USE_DNS: Boolean = false

  private val ZOOKEEPER_SERVER: String = "localhost:2181"
  private val BROKERS_PATH: String = "/brokers/ids"
  private val SCHEMA_REGISTRY: String = "/schema_registry/schema_registry_master"
  private val HTTP_PROTOCOL = "http://"
  private val SCHEMA_HOST: String = "localhost:8081"

  val gson = new Gson

  def brokers: String = {
    val brokers = new ListBuffer[String]()

    Using(new ZooKeeper(ZOOKEEPER_SERVER, 10000, event =>
      logger.debug(s"type:${event.getType}, state:${event.getState}, path:${event.getPath}"))) { zk =>
      zk.getChildren(BROKERS_PATH, true).asScala foreach { id =>
        val brokerInfo = new String(zk.getData(s"$BROKERS_PATH/$id", true, null))
        logger.debug(s"Broker: $id info: $brokerInfo")
        val broker = gson.fromJson(brokerInfo, classOf[KafkaBrokerInfo])
        brokers += s"${broker.host}:${broker.port}"
      }
    }.toEither match {
      case Right(_) => logger.info("Brokers hosts success.")
      case Left(e) => logger.error("Brokers hosts error.", e)
    }

    brokers.mkString(",")
  }

  def registryUrl(dns: Boolean): String = {
    var schemaRegistryUrl = s"$HTTP_PROTOCOL$SCHEMA_HOST"

    Using(new ZooKeeper(ZOOKEEPER_SERVER, 10000, event =>
      logger.debug(s"type:${event.getType}, state:${event.getState}, path:${event.getPath}"))) { zk =>
      val registry = new String(zk.getData(SCHEMA_REGISTRY, true, null))
      val registryData = gson.fromJson(registry, classOf[KafkaBrokerInfo])
      if (dns)
        schemaRegistryUrl = s"$HTTP_PROTOCOL${registryData.host}:${registryData.port}"
    }.toEither match {
      case Right(_) => logger.info("Registry Schema success.")
      case Left(e) => logger.error("Registry Schema error.", e)
    }

    schemaRegistryUrl
  }
}
