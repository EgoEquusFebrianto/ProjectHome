from collections import namedtuple
from pyspark.sql import SparkSession
from lib.logging import Log4j
import os

os.environ['SPARK_LOCAL_IP'] = "IP"

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
    file_log = "-Dlogfile.name=RDD-spark-app"

    spark = SparkSession.builder \
        .appName("Spark RDD App") \
        .config("spark.master", "local[3]") \
        .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}") \
        .getOrCreate()

    logger = Log4j(spark)
    sc = spark.sparkContext

    linesRDD = sc.textFile("data/experiment_sample.csv")
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filterRDD = selectRDD.filter(lambda value: value.Age < 40)
    kvRDD = filterRDD.map(lambda value: (value.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)
    colList = countRDD.collect()

    for x in colList:
        logger.info(x)

    spark.stop()