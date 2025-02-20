from pyspark.sql import SparkSession

class Log4j:
    def __init__(self, spark: SparkSession):
        log4j = spark._jvm.org.apache.logging.log4j
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        namespace = "engineer.capstoneproject.spark.application"

        self.logger = log4j.LogManager.getLogger(f"{namespace}.{app_name}")

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def warn(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)