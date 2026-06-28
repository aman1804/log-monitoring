from pyspark.sql.types import *

nginx_schema = StructType([
    StructField("raw_log", StringType(), True)
])