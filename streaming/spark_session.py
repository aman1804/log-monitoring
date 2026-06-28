from pyspark.sql import SparkSession


def get_spark():

    spark = (
        SparkSession.builder
        .appName("Real-Time Log Monitoring")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark