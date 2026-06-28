import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="logdb",
    user="postgres",
    password="postgres"
)

conn.autocommit = False

cur = conn.cursor()


def insert_dataframe(batch_df, table_name, columns, batch_id):

    rows = batch_df.collect()

    print("\n" + "=" * 60)
    print(f"Batch ID : {batch_id}")
    print(f"Table    : {table_name}")
    print(f"Rows     : {len(rows)}")
    print("=" * 60)

    if len(rows) == 0:
        return

    placeholders = ",".join(["%s"] * len(columns))

    query = f"""
        INSERT INTO {table_name}
        ({",".join(columns)})
        VALUES ({placeholders})
    """

    values = []

    for row in rows:

        values.append(

            tuple(
                getattr(row, column)
                for column in columns
            )

        )

    cur.executemany(query, values)

    conn.commit()

    print(f"{len(values)} rows inserted into {table_name}")
    
    

# ------------------------------------------
# Common Refresh Function
# ------------------------------------------

def refresh_dataframe(batch_df, table_name, columns, batch_id):

    rows = batch_df.collect()

    print(f"\nBatch : {batch_id}")
    print(f"Refreshing : {table_name}")
    print(f"Rows : {len(rows)}")

    cur.execute(f"TRUNCATE TABLE {table_name}")

    if len(rows) == 0:
        conn.commit()
        return

    placeholders = ",".join(["%s"] * len(columns))

    query = f"""
        INSERT INTO {table_name}
        ({",".join(columns)})
        VALUES ({placeholders})
    """

    values = [
        tuple(getattr(row, c) for c in columns)
        for row in rows
    ]

    cur.executemany(query, values)

    conn.commit()

    print(f"{len(values)} rows refreshed in {table_name}")



# ---------------------------------------------------
# ACCESS LOGS
# ---------------------------------------------------

def write_access_logs(batch_df, batch_id):

    insert_dataframe(
        batch_df=batch_df,
        table_name="access_logs",
        columns=[
            "ip",
            "timestamp",
            "method",
            "endpoint",
            "protocol",
            "status",
            "bytes",
            "referer",
            "user_agent"
        ],
        batch_id=batch_id
    )


# ---------------------------------------------------
# ERROR LOGS
# ---------------------------------------------------

def write_error_logs(batch_df, batch_id):

    insert_dataframe(
        batch_df=batch_df,
        table_name="error_logs",
        columns=[
            "ip",
            "timestamp",
            "method",
            "endpoint",
            "protocol",
            "status",
            "error_type",
            "user_agent"
        ],
        batch_id=batch_id
    )


# ---------------------------------------------------
# TOP IPS
# ---------------------------------------------------

def write_top_ips(batch_df, batch_id):

    refresh_dataframe(
        batch_df=batch_df,
        table_name="top_ips",
        columns=[
            "ip",
            "request_count",
            "last_updated"
        ],
        batch_id=batch_id
    )


# ---------------------------------------------------
# TOP ENDPOINTS
# ---------------------------------------------------

def write_top_endpoints(batch_df, batch_id):

    refresh_dataframe(
        batch_df=batch_df,
        table_name="top_endpoints",
        columns=[
            "endpoint",
            "request_count",
            "last_updated"
        ],
        batch_id=batch_id
    )


# ---------------------------------------------------
# REQUESTS PER MINUTE
# ---------------------------------------------------

def write_requests_per_minute(batch_df, batch_id):

    refresh_dataframe(
        batch_df=batch_df,
        table_name="requests_per_minute",
        columns=[
            "window_start",
            "window_end",
            "request_count"
        ],
        batch_id=batch_id
    )


# ---------------------------------------------------
# ERRORS PER MINUTE
# ---------------------------------------------------

def write_errors_per_minute(batch_df, batch_id):

    refresh_dataframe(
        batch_df=batch_df,
        table_name="errors_per_minute",
        columns=[
            "window_start",
            "window_end",
            "error_count"
        ],
        batch_id=batch_id
    )


# ---------------------------------------------------
# ALERTS
# ---------------------------------------------------

def write_alerts(batch_df, batch_id):

    insert_dataframe(
        batch_df=batch_df,
        table_name="alerts",
        columns=[
            "alert_type",
            "severity",
            "description",
            "alert_time"
        ],
        batch_id=batch_id
    )