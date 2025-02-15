from pyspark.sql import SparkSession
from lib.ConfigLoader import get_spark_configs


def get_spark_session(env):
    location = "-Dlog4j.configurationFile=file:log4j2.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=app-logs"
    file_log = "-Dlogfile.name=CapstoneProject-app"

    if env == "local":
        spark = SparkSession.builder \
            .config(conf=get_spark_configs(env)) \
            .config("spark.sql.autoBroadcastJoinThreshold", -1) \
            .config("spark.sql.adaptive.enabled", False) \
            .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}") \
            .enableHiveSupport() \
            .getOrCreate()

    else:
        spark = SparkSession.builder \
            .config(conf=get_spark_configs(env)) \
            .enableHiveSupport() \
            .getOrCreate()

    return spark
