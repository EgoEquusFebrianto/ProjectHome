from configparser import ConfigParser
from pyspark import SparkConf
from pyspark.sql.dataframe import  DataFrame

def get_spark_configuration():
    spark_conf = SparkConf()
    config = ConfigParser()
    config.read("spark.conf")

    for (key, value) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, value)
    return spark_conf

def read_data(engine, path) -> DataFrame:
    data = engine.read \
            .format("csv") \
            .option("header", True) \
            .option("inferschema", True) \
            .load(path)


    return data