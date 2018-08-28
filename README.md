# spark-structured-streaming-on-iae-to-scylladb
Spark Structured Streaming from IBM Message Hub to ScyllaDB using Spark on IBM Analytics Engine

# Introduction

The purpose of this project is to take the continuous data set produced to IBM Message Hub (Kafka) by [dataset-generator](https://github.com/ibm-cloud-streaming-retail-demo/kafka-producer-for-simulated-data
) and storing the data in IBM Compose ScyllaDB using Apache Spark Structured Streaming on IBM Analytics Engine.

**!WARNING!** This project only works with Apache Spark 2.2.x whereas IBM Analytics Engine has Apache Spark 2.3.x.  This project will not yet run on IBM Analytics Engine.

# Acknowledgements

This project is based on https://github.com/polomarcus/Spark-Structured-Streaming-Examples

# Prerequisites

- You have an IBM Cloud account
- You have followed the instructions in the project [kafka-producer-for-simulated-data](https://github.com/ibm-cloud-streaming-retail-demo/kafka-producer-for-simulated-data) to create a continuous stream of data on Kafka
- You have an IBM Compose ScyllaDB instance running in IBM Cloud
- You have an IBM Analytics Engine (1.1) instance running in IBM Cloud
- You have SBT 1.2.1+ installed ([instructions](https://www.scala-sbt.org/1.x/docs/Setup.html))
- You have Cassandra cqsh command installed and configured ([instructions](https://console.bluemix.net/docs/services/ComposeForScyllaDB/scylla-cqlsh.html#using-cqlsh)

# Optional

- You have scala knowledge (you will only need this if you want to change the demo functionality)

# Setup

- Clone this project

```
git clone https://github.com/ibm-cloud-streaming-retail-demo/spark-structured-streaming-on-iae-to-scylladb
cd spark-structured-streaming-on-iae-to-scylladb/
```

- Copy the template files:

```
cp ./jaas_mh.conf_template jaas_mh.conf
cp ./cassandra.properties_template cassandra.properties
cp ./messagehub.properties_template messagehub.properties
```

- Edit `jaas_mh.conf` with your Message Hub username and password
- Edit `cassandra.properties` with your ScyllaDB connection details
- Edit `messagehub.properties` with your MessageHub connection details

- Open a cqlsh session and paste the contents of [schema.sql](./schema.sql) to create the scyllaDB schema.

# Running

```
rm -rf checkpoint/ && SBT_OPTS="-XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=6G -Xmx6G" sbt clean run
```

# Developing

Developed with Intellij IDEA
