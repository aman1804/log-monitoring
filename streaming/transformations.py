from pyspark.sql.functions import *


# ----------------------------------------------------------
# ERROR LOGS
# ----------------------------------------------------------

def get_error_logs(df):

    return (
        df.filter(col("status") >= 400)
        .withColumn(
            "error_type",
            when(col("status") == 404, "Not Found")
            .when(col("status") == 400, "Bad Request")
            .when(col("status") == 401, "Unauthorized")
            .when(col("status") == 403, "Forbidden")
            .when(col("status") == 500, "Internal Server Error")
            .when(col("status") == 502, "Bad Gateway")
            .when(col("status") == 503, "Service Unavailable")
            .otherwise("Other Error")
        )
    )


def get_server_errors(df):

    return df.filter(col("status") >= 500)


def get_client_errors(df):

    return df.filter(col("status") >= 400)


# ----------------------------------------------------------
# TOP ENDPOINTS
# ----------------------------------------------------------

def top_endpoints(df):

    return (
        df.groupBy("endpoint")
        .count()
        .withColumnRenamed(
            "count",
            "request_count"
        )
        .withColumn(
            "last_updated",
            current_timestamp()
        )
        .orderBy(desc("request_count"))
    )


# ----------------------------------------------------------
# TOP IPS
# ----------------------------------------------------------

def top_ips(df):

    return (
        df.groupBy("ip")
        .count()
        .withColumnRenamed(
            "count",
            "request_count"
        )
        .withColumn(
            "last_updated",
            current_timestamp()
        )
        .orderBy(desc("request_count"))
    )


# ----------------------------------------------------------
# REQUESTS PER MINUTE
# ----------------------------------------------------------

def requests_per_minute(df):

    return (
        df.groupBy(
            window(
                col("timestamp"),
                "1 minute"
            )
        )
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("count").alias("request_count")
        )
    )


# ----------------------------------------------------------
# ERRORS PER MINUTE
# ----------------------------------------------------------

def errors_per_minute(df):

    return (
        df.filter(col("status") >= 400)
        .groupBy(
            window(
                col("timestamp"),
                "1 minute"
            )
        )
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("count").alias("error_count")
        )
    )