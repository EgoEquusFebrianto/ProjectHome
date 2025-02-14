from datetime import date
from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from EXP_WorkingWithDataFrameRows_Intro import convert_date
import os

os.environ['SPARK_LOCAL_IP'] = "IP"

class RowDemoTest(TestCase):
    @classmethod
    def setUpClass(cls):
        # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
        location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
        folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
        file_log = "-Dlogfile.name=SparkRows-app"

        cls.spark = (
        SparkSession.builder
            .appName("Spark App")
            .master("local[3]")
            .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}")
            .getOrCreate()
        )
        _schema = StructType([
            StructField("ID", StringType()),
            StructField("Event_Date", StringType())
        ])

        example = [Row("1", "10/2/2025"), Row("2", "11/2/2025"), Row("3", "12/2/2025"), Row("4", "13/2/2025"),
                   Row("5", "14/2/2025")]
        df_rdd = cls.spark.sparkContext.parallelize(example, 2)
        cls.df = cls.spark.createDataFrame(df_rdd, _schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_data_type(self):
        rows = convert_date(self.df, "d/M/y", "Event_Date").collect()
        for row in rows:
            self.assertIsInstance(row["Event_Date"], date)

    def test_data_value(self):
        rows = convert_date(self.df, "d/M/y", "Event_Date").collect()
        for row in rows:
            self.assertEqual(row["Event_Date"], date(2025, 2, 10))