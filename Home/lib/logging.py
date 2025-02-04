from pyspark.sql import SparkSession

class Log4j:
    def __init__(self, spark: SparkSession):
        log4j = spark._jvm.org.apache.logging.log4j
        root_class = "engineer.studiesproject.spark.example"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(f"{root_class}.{app_name}")

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)

    # _instance = None
    #
    # def __new__(cls, spark):
    #     if cls._instance is None:
    #         cls._instance = super(Log4j, cls).__new__(cls)
    #         log4j = spark._jvm.org.apache.logging.log4j
    #         root_class = "engineer.studiesproject.spark.example"
    #
    #         conf = spark.sparkContext.getConf()
    #         app_name = conf.get("spark.app.name")
    #
    #         cls._instance.logger = log4j.LogManager.getLogger(f"{root_class}.{app_name}")
    #     return cls._instance