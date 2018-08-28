package kafka

import org.apache.spark.sql.types._
import spark.SparkHelper

object KafkaService {
  private val spark = SparkHelper.getSparkSession()

  val invoiceItemStructureName = "invoiceItem"

  val topicName = "transactions_load"

  val bootstrapServers = "localhost:9092"

  val schemaOutput = new StructType()
    .add("InvoiceNo", LongType)
    .add("StockCode", LongType)
    .add("Description", StringType)
    .add("Quantity", ShortType)
    .add("InvoiceDate", LongType)
    .add("UnitPrice", DoubleType)
    .add("CustomerID", IntegerType)
    .add("Country", StringType)
    .add("LineNo", ShortType)
    .add("InvoiceTime", StringType)
    .add("StoreID", ShortType)
    .add("TransactionID", StringType)
}
