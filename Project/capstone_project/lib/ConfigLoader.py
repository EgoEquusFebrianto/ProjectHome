from configparser import ConfigParser
from pyspark import SparkConf

def get_spark_configs(environment: str):
    spark_conf, config = SparkConf(), ConfigParser()
    config.read('spark_conf/spark.conf')

    for (key, value) in config.items(environment.upper()):
        spark_conf.set(key, value)

    return spark_conf