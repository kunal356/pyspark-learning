from datetime import date
from unittest import TestCase

from pyspark.sql import *
from pyspark.sql.types import *

from RowDemo import to_date_df


class RowDemoTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("RowDemoTest") \
            .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark") \
            .getOrCreate()

        my_schema = StructType(
            [
                StructField("ID", StringType()),
                StructField("EventDate", StringType())
            ]
        )

        my_rows = [Row("123", "04/05/2020"),
                   Row("124", "04/05/2020"),
                   Row("125", "04/05/2020"),
                   Row("126", "04/05/2020"),
                   ]

        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_data_type(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_data_value(self):
        rows = to_date_df(self.my_df, "M/d/y")
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020, 4, 5))
