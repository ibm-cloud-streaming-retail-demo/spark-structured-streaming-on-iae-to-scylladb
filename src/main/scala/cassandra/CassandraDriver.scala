package cassandra

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import invoices.{InvoiceItem, InvoiceItemKafka}
import kafka.KafkaService
import log.LazyLogger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.StreamSinkProvider
import org.apache.spark.sql.streaming.OutputMode
import spark.SparkHelper

object CassandraDriver extends LazyLogger {
  private val spark = SparkHelper.getSparkSession()
  import spark.implicits._

  val connector = CassandraConnector(SparkHelper.getSparkSession().sparkContext.getConf)

  val namespace = "structuredstreaming"
  val StreamProviderTableSink = "invoiceitem"
  val kafkaMetadata = "kafkametadata"

  /**
    * remove kafka metadata and only focus on business structure
    */
  def getDatasetForCassandra(df: DataFrame) = {
    df.select(KafkaService.invoiceItemStructureName + ".*")
      .as[InvoiceItem]
  }

  def saveStreamSinkProvider(ds: Dataset[InvoiceItemKafka]) = {
    ds
      .toDF() //@TODO see if we can use directly the Dataset object
      .writeStream
      .format("cassandra.ScyllaSinkProvider")
      .outputMode(OutputMode.Append)
      .queryName("KafkaToCassandraStreamSinkProvider")
      .options(
        Map(
          "keyspace" -> namespace,
          "table" -> StreamProviderTableSink,

          // TODO - save checkpoints in scylladb
          "checkpointLocation" -> "/tmp/checkpoints"
        )
      )

      .start()
  }

  /**
    * @TODO handle more topic name, for our example we only use the topic "test"
    *
    *  we can use collect here as kafkameta data is not big at all
    *
    * if no metadata are found, we would use the earliest offsets.
    *
    * @see https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#creating-a-kafka-source-batch
    *  assign	json string {"topicA":[0,1],"topicB":[2,4]}
    *  Specific TopicPartitions to consume. Only one of "assign", "subscribe" or "subscribePattern" options can be specified for Kafka source.
    */
  def getKafaMetadata() = {
    try {
      val kafkaMetadataRDD = spark.sparkContext.cassandraTable(namespace, kafkaMetadata)

      log.warn(kafkaMetadataRDD.take(2))

      val output = if (kafkaMetadataRDD.isEmpty) {
        ("startingOffsets", "earliest")
      } else {
        ("startingOffsets", transformKafkaMetadataArrayToJson(kafkaMetadataRDD.collect()))
      }
      log.warn("getKafkaMetadata " + output.toString)

      output
    }
    catch {
      case e: Exception =>
        ("startingOffsets", "earliest")
    }
  }

  /**
    * @param array
    * @return {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}
    */
  def transformKafkaMetadataArrayToJson(array: Array[CassandraRow]) : String = {
      s"""{"${KafkaService.topicName}":
          {
           "${array(0).getLong("partition")}": ${array(0).getLong("offset")}
          }
         }
      """.replaceAll("\n", "").replaceAll(" ", "")
  }
}

class ScyllaSinkProvider extends StreamSinkProvider {
  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): ScyllaSink =
    new ScyllaSink(parameters)
}

class ScyllaSink(parameters: Map[String, String]) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    val schema = data.schema

    val rdd: RDD[Row] = data.queryExecution.toRdd.mapPartitions { rows =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      rows.map(converter(_).asInstanceOf[Row])
    }

    val spark = SparkHelper.getSparkSession()
    val newDF = spark.sqlContext.createDataFrame(rdd, schema)

    newDF.select(
      newDF.col("invoiceitem.InvoiceNo"),
      newDF.col("invoiceitem.StockCode"),
      newDF.col("invoiceitem.Description"),
      newDF.col("invoiceitem.Quantity"),
      newDF.col("invoiceitem.InvoiceDate"),
      newDF.col("invoiceitem.UnitPrice"),
      newDF.col("invoiceitem.CustomerID"),
      newDF.col("invoiceitem.Country"),
      newDF.col("invoiceitem.LineNo"),
      newDF.col("invoiceitem.InvoiceTime"),
      newDF.col("invoiceitem.StoreID"),
      newDF.col("invoiceitem.TransactionID")
    ).write.cassandraFormat(
      parameters("table"),
      parameters("keyspace")
    ).mode(SaveMode.Append).save()
  }
}