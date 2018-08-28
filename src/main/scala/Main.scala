package main

import cassandra.CassandraDriver
import kafka.KafkaSource
import spark.SparkHelper

object Main {

  def main(args: Array[String]) {

    val spark = SparkHelper.getAndConfigureSparkSession()

    //Finally read it from kafka, in case checkpointing is not available we read last offsets saved from Cassandra
    val (startingOption, partitionsAndOffsets) = CassandraDriver.getKafaMetadata()
    val kafkaInputDS = KafkaSource.read(startingOption, partitionsAndOffsets)

    //Just debugging Kafka source into our console
    KafkaSource.debugStream(kafkaInputDS)

    //Saving using Datastax connector's saveToCassandra method
    CassandraDriver.saveStreamSinkProvider(kafkaInputDS)

    //Wait for all streams to finish
    spark.streams.awaitAnyTermination()
  }
}
