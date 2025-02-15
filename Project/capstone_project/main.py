from lib.logging import Log4j
from lib.utils import get_spark_session

mode = {1:"local", 2:"qa", 3:"prod"}

if __name__ == "__main__":
    spark = get_spark_session(mode[1])
    logger = Log4j(spark)

    logger.info("Spark App is Running...")



    logger.info("Spark Terminate")
    spark.stop()