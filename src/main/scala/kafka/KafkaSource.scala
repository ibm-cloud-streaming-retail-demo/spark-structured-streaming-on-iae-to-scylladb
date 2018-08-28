package kafka

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{struct, to_json, _}
import _root_.log.LazyLogger
import org.apache.spark.sql.types.{StringType, _}
import invoices.{InvoiceItem, InvoiceItemKafka}
import spark.SparkHelper

/**
 @see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
 */
object KafkaSource extends LazyLogger {
  private val spark = SparkHelper.getSparkSession()

  import spark.implicits._

  /**
    * will return, we keep some kafka metadata for our example, otherwise we would only focus on "radioCount" structure
     |-- key: binary (nullable = true)
     |-- value: binary (nullable = true)
     |-- topic: string (nullable = true) : KEPT
     |-- partition: integer (nullable = true) : KEPT
     |-- offset: long (nullable = true) : KEPT
     |-- timestamp: timestamp (nullable = true) : KEPT
     |-- timestampType: integer (nullable = true)
     |-- invoiceItem: struct (nullable = true)
     |    |-- ...

    * @return
    *
    *
    * startingOffsets should use a JSON coming from the lastest offsets saved in our DB (Cassandra here)
    */
    def read(startingOption: String = "startingOffsets", partitionsAndOffsets: String = "earliest") : Dataset[InvoiceItemKafka] = {

      val (bootstrapServers) =
        try {
          val prop = new Properties()
          prop.load(new FileInputStream("messagehub.properties"))
          (
            prop.getProperty("bootstrap.servers")
          )
        } catch { case e: Exception =>
          e.printStackTrace()
          sys.exit(1)
        }

      log.warn("Reading from Kafka")

      spark
      .readStream
      .format("kafka")

      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", "transactions_load")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.ssl.protocol", "TLSv1.2")
      .option("kafka.ssl.enabled.protocols", "TLSv1.2")

      .option("enable.auto.commit", false) // Cannot be set to true in Spark Strucutured Streaming https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
      .option("group.id", "Structured-Streaming-Examples")
      .option("failOnDataLoss", false) // when starting a fresh kafka (default location is temporary (/tmp) and cassandra is not (var/lib)), we have saved different offsets in Cassandra than real offsets in kafka (that contains nothing)
      .option(startingOption, partitionsAndOffsets) //this only applies when a new query is started and that resuming will always pick up from where the query left off
      .load()

      .withColumn(KafkaService.invoiceItemStructureName, // nested structure with our json
        from_json($"value".cast(StringType), KafkaService.schemaOutput) //From binary to JSON object
      ).as[InvoiceItemKafka]
      .filter(_.invoiceItem != null)
      .filter(_.invoiceItem.InvoiceNo != null) //TODO find a better way to filter bad json
  }

  /**
    *Console sink from Kafka's stream
      *+----+--------------------+-----+---------+------+--------------------+-------------+--------------------+
      *| key|               value|topic|partition|offset|           timestamp|timestampType|          radioCount|
      *+----+--------------------+-----+---------+------+--------------------+-------------+--------------------+
      *|null|[7B 22 72 61 64 6...| test|        0|    60|2017-11-21 22:56:...|            0|[Feel No Ways,Dra...|
    *
    */
  def debugStream(staticKafkaInputDS: Dataset[InvoiceItemKafka]) = {
    staticKafkaInputDS
      .writeStream
      .queryName("Debug Stream Kafka")
      .format("console")
      .start()
  }
}
