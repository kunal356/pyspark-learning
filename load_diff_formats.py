from pyspark.sql import *

from lib.logger import Log4j
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()

    spark = SparkSession \
        .builder \
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark") \
        .config(conf=conf) \
        .getOrCreate()
    
    logger = Log4j(spark)
        
    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .load("data/flight*.csv")
    
    flightTimeCsvDF.show(5)
    logger.info("CSV Schema:" + flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format("json") \
        .option("header", "true") \
        .load("data/flight*.json")
    
    flightTimeJsonDF.show(5)
    logger.info("Json Schema:" + flightTimeJsonDF.schema.simpleString())

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .option("header", "true") \
        .load("data/flight*.parquet")
    
    flightTimeParquetDF.show(5)
    logger.info("CSV Schema:" + flightTimeParquetDF.schema.simpleString())