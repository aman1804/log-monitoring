from pyspark.sql.functions import col
from streaming.spark_session import get_spark
# from database.postgres import write_access_logs
from streaming.kafka_reader import read_logs
from streaming.parser import (
    parse_json,
    parse_access_logs
)
from database.postgres import (
    write_access_logs,
    write_error_logs,
    write_top_ips,
    write_top_endpoints,
    write_requests_per_minute,
    write_errors_per_minute,
    write_alerts
)
from streaming.transformations import (
    get_error_logs,
    top_endpoints,
    top_ips,
    requests_per_minute,
    errors_per_minute
)

from streaming.alerts import (
    generate_alerts,
    create_alerts
)
spark = get_spark()

raw_df = read_logs(spark)

# logs = raw_df.select(
#     col("value").cast("string").alias("message")
# )
logs = parse_json(raw_df)

logs = parse_access_logs(logs)

error_logs = get_error_logs(logs)

top_urls = top_endpoints(logs)

top_ip = top_ips(logs)

rpm = requests_per_minute(logs)

epm = errors_per_minute(logs)

alert_stats = generate_alerts(logs)

alerts = create_alerts(alert_stats)

access_query = (
    logs.writeStream
    .foreachBatch(write_access_logs)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/access_logs"
    )
    .start()
)
error_query = (
    error_logs.writeStream
    .foreachBatch(write_error_logs)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "checkpoints/error_logs"
    )
    .start()
)

endpoint_query = (
    top_urls.writeStream
    .foreachBatch(write_top_endpoints)
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "checkpoints/top_endpoints"
    )
    .start()
)

ip_query = (
    top_ip.writeStream
    .foreachBatch(write_top_ips)
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "checkpoints/top_ips"
    )
    .start()
)

rpm_query = (
    rpm.writeStream
    .foreachBatch(write_requests_per_minute)
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "checkpoints/rpm"
    )
    .start()
)

epm_query = (
    epm.writeStream
    .foreachBatch(write_errors_per_minute)
    .outputMode("complete")
    .option(
        "checkpointLocation",
        "checkpoints/errors_per_minute"
    )
    .start()
)

alerts_query = (

    alerts.writeStream

    .foreachBatch(write_alerts)

    .outputMode("complete")

    .option(
        "checkpointLocation",
        "checkpoints/alerts"
    )

    .start()

)
spark.streams.awaitAnyTermination()