from pyspark.sql.functions import *


def generate_alerts(logs):

    return (
        logs.groupBy(
            window(col("timestamp"), "1 minute")
        )
        .agg(

            count("*").alias("total_requests"),

            sum(
                when(col("status") >= 400, 1)
                .otherwise(0)
            ).alias("total_errors"),

            sum(
                when(col("status") == 404, 1)
                .otherwise(0)
            ).alias("not_found"),

            sum(
                when(col("status") >= 500, 1)
                .otherwise(0)
            ).alias("server_errors")
        )
    )
    



def create_alerts(df):

    return (
        df.withColumn(

            "alert_type",

            when(
                col("server_errors") >= 20,
                "HIGH_SERVER_ERRORS"
            )

            .when(
                col("not_found") >= 50,
                "HIGH_404_ERRORS"
            )

            .when(
                col("total_requests") >= 1000,
                "HIGH_TRAFFIC"
            )

            .otherwise(None)

        )

        .filter(col("alert_type").isNotNull())

        .withColumn(

            "severity",

            when(
                col("alert_type") == "HIGH_SERVER_ERRORS",
                "CRITICAL"
            )

            .when(
                col("alert_type") == "HIGH_TRAFFIC",
                "HIGH"
            )

            .otherwise("MEDIUM")

        )

        .withColumn(

            "description",

            concat_ws(

                " ",

                lit("Requests:"),

                col("total_requests"),

                lit("| Errors:"),

                col("total_errors")

            )

        )

        .withColumn(
            "alert_time",
            current_timestamp()
        )

        .select(
            "alert_type",
            "severity",
            "description",
            "alert_time"
        )

    )