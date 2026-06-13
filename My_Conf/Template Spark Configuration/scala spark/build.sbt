ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkScalaTraining",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
      "org.apache.spark" %% "spark-hive" % "3.5.3",

      "org.slf4j" % "slf4j-api" % "2.0.13",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
      "org.apache.logging.log4j" % "log4j-api" % "2.23.1",

      "org.postgresql" % "postgresql" % "42.5.1"
    )
  )