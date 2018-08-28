package spark

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkHelper {

  System.setProperty("java.security.auth.login.config", "jaas_mh.conf")

  val (scyllaHost, scyllaPort, scyllaUsername, scyllaPassword) =
    try {
      val prop = new Properties()
      prop.load(new FileInputStream("cassandra.properties"))
      (
        prop.getProperty("host"),
        prop.getProperty("port"),
        prop.getProperty("username"),
        prop.getProperty("password")
      )
    } catch { case e: Exception =>
      e.printStackTrace()
      sys.exit(1)
    }

  val conf = new SparkConf()
    .setAppName("Structured Streaming from Message Hub to ScyllaDB")
    .setMaster("local[1]")
    .set("spark.sql.streaming.checkpointLocation", "checkpoint")
    // ScyllaDB options
    .set("spark.cassandra.connection.host", scyllaHost)
    .set("spark.cassandra.connection.port", scyllaPort)
    .set("spark.cassandra.auth.username", scyllaUsername)
    .set("spark.cassandra.auth.password", scyllaPassword)
    .set("spark.cassandra.connection.ssl.enabled", "true")
    // Message Hub options
    .set("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=jaas_mh.conf -XX:MaxPermSize=1024m -XX:PermSize=256m")
    .set("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=jaas_mh.conf -XX:MaxPermSize=1024m -XX:PermSize=256m")

  val spark = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  spark.sparkContext.addFile("jaas_mh.conf")

  println(spark.conf.get("spark.driver.extraJavaOptions"))
  println(spark.conf.get("spark.executor.extraJavaOptions"))


  def getAndConfigureSparkSession() : SparkSession = {
    return spark
  }

  def getSparkSession() : SparkSession = {
    return spark
  }
}
