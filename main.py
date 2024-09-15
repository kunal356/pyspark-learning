import sys

from pyspark.sql import *

from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession \
        .builder \
        .appName("Hello Spark") \
        .config("spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=hello-spark") \
        .config(conf=conf) \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting HelloSpark")

    # Checking for system argument for data file
    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    survey_df = load_survey_df(spark, sys.argv[1])
    partitioned_df = survey_df.repartition(2)
    count_df = count_by_country(survey_df=partitioned_df)
    logger.info(count_df.collect())
    input("Enter")  # Stopping from terminating the program for debugging
    logger.info("Finished HelloSpark")
    spark.stop()
