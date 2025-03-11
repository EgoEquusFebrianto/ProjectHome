ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "first-scala-spark-development",
    libraryDependencies ++= Seq(
      // Spark Dependencies
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",

      // Kafka-Spark Dependencies
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
//      "org.apache.kafka" %% "kafka" % "3.8.0",
//      "org.apache.kafka" % "kafka-clients" % "3.8.0",

      // Log4j2
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",

      // Hadoop
      "org.apache.hadoop" % "hadoop-client" % "3.4.0",
      "org.apache.hadoop" % "hadoop-hdfs" % "3.4.0",
      "org.apache.hadoop" % "hadoop-common" % "3.4.0",

      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",
      // Postgresql
      "org.postgresql" % "postgresql" % "42.5.1"

    )
  )
