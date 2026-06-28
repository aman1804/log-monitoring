from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC
)


def read_logs(spark):

    df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "latest"
        )
        .option(
            "failOnDataLoss", 
            "false"
        ) 
        .load()
    )

    return df