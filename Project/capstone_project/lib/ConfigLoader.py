from configparser import ConfigParser
from pyspark import SparkConf

def get_spark_configs(environment: str):
    spark_conf, config = SparkConf(), ConfigParser()
    config.read('spark_conf/spark.conf')

    for (key, value) in config.items(environment.upper()):
        spark_conf.set(key, value)

    return spark_conf

def get_project_configs(environment):
    spark_conf, config = SparkConf(), ConfigParser()
    config.read("spark_conf/project.conf")
    conf = {}

    for (key, value) in config.items(environment.upper()):
        conf[key] = value

    return conf

def get_data_filter(environment, data_filter: str):
    conf = get_project_configs(environment)
    return "true" if conf.get(data_filter) == "" else conf.get(data_filter)