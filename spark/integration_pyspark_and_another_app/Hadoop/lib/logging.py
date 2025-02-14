from pyspark.sql import SparkSession

class Log4j:
    def __init__(self, spark: SparkSession):
        log4j = spark._jvm.org.apache.logging.log4j
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        root_name = "engineer.sparkhadoopproject.spark.example"

        self.logger = log4j.LogManager.getLogger(f"{root_name}.{app_name}")

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def warn(self, message):
        self.logger.warn(message)

    def error(self, message):
        self.logger.error(message)
