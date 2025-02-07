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

def read_data(ext, engine, path, _schema=None) -> DataFrame:
    match ext:
        case "csv":
            if _schema:
                data = engine.read.format(ext) \
                    .option("header", True) \
                    .schema(_schema) \
                    .option("mode", "FAILFAST") \
                    .option("dateFormat", "M/d/y") \
                    .load(path)
            else:
                data = engine.read.format(ext) \
                    .option("header", True) \
                    .option("inferschema", True) \
                    .load(path)

        case "json":
            if _schema:
                data = engine.read.format(ext) \
                    .schema(_schema) \
                    .option("mode", "FAILFAST") \
                    .option("dateFormat", "M/d/y") \
                    .load(path)
            else:
                data = engine.read.format(ext) \
                    .option("inferschema", True) \
                    .load(path)

        case "parquet":
            data = engine.read.format(ext).load(path)

    return data

def count_by_country(data: DataFrame):
    return data.groupby("Country").count()
