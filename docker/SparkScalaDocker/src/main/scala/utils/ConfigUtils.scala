package utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import java.io.File
import scala.collection.mutable

object ConfigUtils {
  def getSparkConfig(cluster: String): SparkConf = {
    val file = new File("/app/config/application.conf")
    val config: Config = ConfigFactory.parseFile(file)
    val sparkConf = new SparkConf()

    config.getConfig(s"spark.$cluster")
      .entrySet()
      .forEach( entry => sparkConf.set(entry.getKey, entry.getValue.unwrapped().toString))

    sparkConf
  }

  def getBatchConfig(action: String): mutable.HashMap[String, String] = {
    val settings: mutable.HashMap[String, String] = mutable.HashMap.empty
    val file = new File("/app/config/application.conf")
    val config = ConfigFactory.parseFile(file)

    config.getConfig(s"variables.$action")
      .entrySet()
      .forEach( entry => settings.put(entry.getKey, entry.getValue.unwrapped().toString))

    settings
  }
}