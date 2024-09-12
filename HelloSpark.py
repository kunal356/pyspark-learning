from pyspark.sql import *

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[2]") \
        .getOrCreate()
    
    data_list = [
        ("Ravi", 28),
        ("Dhruv", 45),
        ("Atul",56)
    ]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()