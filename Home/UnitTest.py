from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import  *
import os

os.environ["SPARK_LOCAL_IP"] = "172.25.5.7"

class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder
                     .config("spark.app.name", "Spark App")
                     .config("spark.local", "local[3]")
                     .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_data_file_loading (self):
        sample_df = read_data(self.spark, "data/sample.csv")
        result_count = sample_df.count()

        self.assertEqual(result_count, 9, "Record count should be 9.")

    def test_country_count (self):
        sample_df = read_data(self.spark, "data/sample.csv")
        count_list = count_by_country(sample_df).collect()
        count_dict = dict()
        for row in count_list:
            count_dict[row["Country"]] = row['count']

        self.assertEqual(count_dict["United States"], 6,"Count for US should be 6")
        self.assertEqual(count_dict["Canada"], 2,"Count for Canada should be 2")
        self.assertEqual(count_dict["United Kingdom"], 1,"Count for UK should be 1")