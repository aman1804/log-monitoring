# 🚀 Real-Time Log Monitoring System using Kafka, Spark Structured Streaming, PostgreSQL, Grafana & Airflow

## 📌 Overview

The **Real-Time Log Monitoring System** is an end-to-end streaming data engineering project that collects Nginx access logs, streams them through Apache Kafka, processes them in real time using Apache Spark Structured Streaming, stores analytical results in PostgreSQL, visualizes metrics using Grafana, and automates reporting tasks with Apache Airflow.

This project demonstrates how modern data engineering technologies work together to build a scalable real-time monitoring platform.

---

# 🏗️ Architecture

```
                    Nginx
                      │
                      ▼
            Python Log Producer
                      │
                      ▼
                 Apache Kafka
                      │
                      ▼
      Spark Structured Streaming
                      │
      ┌───────────────┴───────────────┐
      ▼                               ▼
 Log Parsing                  Alert Generation
      │                               │
      └───────────────┬───────────────┘
                      ▼
                 PostgreSQL
                      │
              ┌───────┴────────┐
              ▼                ▼
          Grafana         Apache Airflow
```

---

# ✨ Features

* Real-time log streaming using Apache Kafka
* Spark Structured Streaming for continuous data processing
* Parsing of Nginx access logs
* Detection of HTTP client (4xx) and server (5xx) errors
* Top requested endpoints analysis
* Top client IP analysis
* Requests per minute analytics
* Errors per minute analytics
* Alert generation based on configurable thresholds
* PostgreSQL storage for analytics
* Grafana dashboards for monitoring
* Apache Airflow for scheduled reporting jobs
* Fully containerized using Docker

---

# 🛠 Tech Stack

| Category             | Technology                        |
| -------------------- | --------------------------------- |
| Programming Language | Python                            |
| Streaming Platform   | Apache Kafka                      |
| Stream Processing    | Apache Spark Structured Streaming |
| Database             | PostgreSQL                        |
| Dashboard            | Grafana                           |
| Workflow Scheduler   | Apache Airflow                    |
| Containerization     | Docker & Docker Compose           |
| Web Server           | Nginx                             |

---

# 📂 Project Structure

```
logs/

├── producer/
│
├── streaming/
│   ├── kafka_reader.py
│   ├── parser.py
│   ├── transformations.py
│   ├── alerts.py
│   ├── spark_session.py
│   └── main.py
│
├── database/
│   └── postgres.py
│
├── airflow/
│   └── dags/
│
├── config/
│
├── docker-compose.yml
│
├── requirements.txt
│
└── README.md
```

---

# 📊 Database Tables

### Transaction Tables

* access_logs
* error_logs

### Analytics Tables

* top_ips
* top_endpoints
* requests_per_minute
* errors_per_minute

### Monitoring Tables

* alerts
* daily_summary

---

# ⚙️ Pipeline Workflow

1. Nginx generates access logs.
2. Python producer reads log entries.
3. Logs are published to Kafka.
4. Spark Structured Streaming consumes Kafka messages.
5. Raw logs are parsed into structured columns.
6. Analytics and alert rules are applied.
7. Processed data is stored in PostgreSQL.
8. Grafana visualizes the data.
9. Airflow executes scheduled reporting jobs.

---

# 📈 Analytics Generated

* Top IP Addresses
* Top Requested Endpoints
* Total Errors
* HTTP Status Distribution
* Requests Per Minute
* Errors Per Minute
* Daily Summary
* Alerts

---

# 🚨 Alert Engine

The system generates alerts for:

* High 500 Errors
* High 404 Errors
* High Traffic
* Endpoint Abuse
* Suspicious Client Activity

---

# 📊 Grafana Dashboard

Dashboard includes:

* Total Requests
* Total Errors
* Active Alerts
* Top Endpoints
* Top IPs
* Request Trend
* Error Trend
* Latest Alerts

---

# ⏰ Airflow Jobs

| DAG           | Purpose                   |
| ------------- | ------------------------- |
| daily_summary | Generate daily statistics |
| cleanup_logs  | Remove old logs           |
| archive_logs  | Archive historical logs   |
| email_report  | Send scheduled reports    |

---

# 🚀 Future Enhancements

* Slack Notifications
* Microsoft Teams Alerts
* ELK Stack Integration
* Prometheus Metrics
* Kubernetes Deployment
* AWS Cloud Deployment
* Machine Learning Based Anomaly Detection

---

# 👨‍💻 Author

**Aman Dubey**

Data Engineer | ETL Developer | PySpark | Kafka | Spark Structured Streaming | PostgreSQL | Docker | Airflow
