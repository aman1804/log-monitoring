from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2


def generate_daily_summary():

    conn = psycopg2.connect(
        host="host.docker.internal",
        port=5433,
        database="logdb",
        user="postgres",
        password="postgres"
    )

    cur = conn.cursor()

    cur.execute("""

        INSERT INTO daily_summary
        (
            summary_date,
            total_requests,
            total_errors,
            total_404,
            total_500,
            unique_ips
        )

        SELECT

            CURRENT_DATE,

            COUNT(*) AS total_requests,

            SUM(
                CASE
                    WHEN status >= 400 THEN 1
                    ELSE 0
                END
            ) AS total_errors,

            SUM(
                CASE
                    WHEN status = 404 THEN 1
                    ELSE 0
                END
            ) AS total_404,

            SUM(
                CASE
                    WHEN status >= 500 THEN 1
                    ELSE 0
                END
            ) AS total_500,

            COUNT(DISTINCT ip) AS unique_ips

        FROM access_logs

        WHERE DATE(timestamp)=CURRENT_DATE;

    """)

    conn.commit()

    cur.close()

    conn.close()

    print("Daily Summary Generated Successfully")


with DAG(

    dag_id="daily_summary",

    start_date=datetime(2026, 1, 1),

    schedule="0 0 * * *",

    catchup=False,

    tags=["logs", "postgres"]

) as dag:

    daily_summary = PythonOperator(

        task_id="generate_daily_summary",

        python_callable=generate_daily_summary

    )

    daily_summary