from pyspark.sql.functions import *


def parse_json(df):

    return (
        df.select(
            from_json(
                col("value").cast("string"),
                "raw_log STRING"
            ).alias("json")
        )
        .select("json.*")
    )


def parse_access_logs(df):

    access = df.filter(
        col("raw_log").rlike(r"^\d+\.\d+\.\d+\.\d+")
    )

    return (
        access

        # IP
        .withColumn(
            "ip",
            regexp_extract(
                col("raw_log"),
                r'^(\S+)',
                1
            )
        )

        # Timestamp
        .withColumn(
            "timestamp",
            to_timestamp(
                regexp_extract(
                    col("raw_log"),
                    r'\[(.*?)\]',
                    1
                ),
                "dd/MMM/yyyy:HH:mm:ss Z"
            )
        )

        # Method
        .withColumn(
            "method",
            regexp_extract(
                col("raw_log"),
                r'"([A-Z]+)',
                1
            )
        )

        # Endpoint
        .withColumn(
            "endpoint",
            regexp_extract(
                col("raw_log"),
                r'"[A-Z]+ (.*?) HTTP',
                1
            )
        )

        # Protocol
        .withColumn(
            "protocol",
            regexp_extract(
                col("raw_log"),
                r'"[A-Z]+ .*? (HTTP\/[0-9.]+)"',
                1
            )
        )

        # Status Code
        .withColumn(
            "status",
            regexp_extract(
                col("raw_log"),
                r'HTTP\/[0-9.]+" (\d+)',
                1
            ).cast("int")
        )

        # Bytes
        .withColumn(
            "bytes",
            regexp_extract(
                col("raw_log"),
                r'HTTP\/[0-9.]+" \d+ (\d+)',
                1
            ).cast("int")
        )

        # Referer
        .withColumn(
            "referer",
            regexp_extract(
                col("raw_log"),
                r'HTTP\/[0-9.]+" \d+ \d+ "(.*?)"',
                1
            )
        )

        # User Agent
        .withColumn(
            "user_agent",
            regexp_extract(
                col("raw_log"),
                r'HTTP\/[0-9.]+" \d+ \d+ ".*?" "(.*?)"',
                1
            )
        )
    )