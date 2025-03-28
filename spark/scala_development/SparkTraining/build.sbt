ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "SparkTraining",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql" % "3.5.3",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.3",
      "org.apache.spark" %% "spark-hive" % "3.5.3",

      "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",
      "org.postgresql" % "postgresql" % "42.5.1"
    )
  )
