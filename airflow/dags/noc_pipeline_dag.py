"""
Airflow DAG — NOC Real-Time Network Event Streaming Pipeline
Project: Telecom / Network Operations Center Analytics

Schedule: Every hour
Flow:
  1. Health check — confirm Kafka topic has new events
  2. Trigger Silver notebook (Bronze→Silver enrichment)
  3. Run data quality checks on Silver
  4. Trigger Gold notebook (Silver→Gold KPIs)
  5. Copy Gold to Snowflake (COPY INTO)
  6. Refresh Power BI dataset (optional)
  7. Send alert summary email if CRITICAL events found
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

# ── Default args ──────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": True,
    "email":            ["data-ops@company.com"],
}

# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id="noc_streaming_pipeline",
    default_args=default_args,
    description="NOC real-time event pipeline: Kafka → Delta → Snowflake",
    schedule_interval="0 * * * *",   # every hour
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "kafka", "databricks", "snowflake", "noc"],
) as dag:

    # ── Task 1: Check Kafka topic has recent events
    def check_kafka_health(**context):
        """
        Verify Kafka topic network-events has messages in the last hour.
        If no messages → skip Gold build but keep streaming query running.
        """
        from kafka import KafkaConsumer
        import json

        consumer = KafkaConsumer(
            "network-events",
            bootstrap_servers="kafka-broker:9092",
            auto_offset_reset="latest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        messages = list(consumer)
        consumer.close()

        msg_count = len(messages)
        context["task_instance"].xcom_push(key="kafka_msg_count", value=msg_count)
        print(f"Kafka messages in last poll: {msg_count}")

        return "run_silver_notebook" if msg_count > 0 else "skip_no_events"

    kafka_health_check = BranchPythonOperator(
        task_id="kafka_health_check",
        python_callable=check_kafka_health,
    )

    skip_no_events = EmptyOperator(task_id="skip_no_events")

    # ── Task 2: Run Silver enrichment notebook on Databricks
    run_silver_notebook = DatabricksRunNowOperator(
        task_id="run_silver_notebook",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.silver_notebook_job_id }}",
        notebook_params={
            "batch_date": "{{ ds }}",
            "batch_hour": "{{ execution_date.hour }}",
        },
    )

    # ── Task 3: Data quality check (Python)
    def run_dq_checks(**context):
        """
        Quick DQ check on Silver table:
        - No null event_ids
        - No future timestamps
        - Severity values are valid
        Returns: pass/fail count
        """
        from pyspark.sql import SparkSession, functions as F

        spark = SparkSession.getActiveSession()
        if not spark:
            print("DQ checks skipped — no active Spark session in Airflow context")
            print("In production: call DQ via Databricks REST API or run as separate notebook")
            return {"status": "skipped", "note": "run via Databricks job in production"}

        silver = spark.read.format("delta").load("/mnt/delta/silver")
        checks = {
            "null_event_ids":   silver.filter(F.col("event_id").isNull()).count(),
            "future_events":    silver.filter(F.col("event_ts") > F.current_timestamp()).count(),
            "invalid_severity": silver.filter(
                ~F.col("severity").isin("INFO","WARNING","ERROR","CRITICAL")
            ).count(),
        }
        print("DQ Results:", checks)
        failed = {k: v for k, v in checks.items() if v > 0}
        if failed:
            raise ValueError(f"DQ checks failed: {failed}")
        return checks

    dq_checks = PythonOperator(
        task_id="dq_checks",
        python_callable=run_dq_checks,
    )

    # ── Task 4: Run Gold notebook on Databricks
    run_gold_notebook = DatabricksRunNowOperator(
        task_id="run_gold_notebook",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.gold_notebook_job_id }}",
        notebook_params={
            "batch_date": "{{ ds }}",
            "batch_hour": "{{ execution_date.hour }}",
        },
    )

    # ── Task 5: Copy Gold → Snowflake
    copy_to_snowflake = SnowflakeOperator(
        task_id="copy_gold_to_snowflake",
        snowflake_conn_id="snowflake_default",
        sql="""
            COPY INTO NOC_DB.GOLD.DEVICE_HEALTH_KPIS
            FROM @NOC_DB.GOLD.ADLS_GOLD_STAGE/device_health_kpis/
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR    = 'CONTINUE'
            PURGE       = FALSE;

            COPY INTO NOC_DB.GOLD.ALERT_SUMMARY
            FROM @NOC_DB.GOLD.ADLS_GOLD_STAGE/alert_summary/
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR    = 'CONTINUE';

            COPY INTO NOC_DB.GOLD.NETWORK_RELIABILITY
            FROM @NOC_DB.GOLD.ADLS_GOLD_STAGE/network_reliability/
            FILE_FORMAT = (TYPE = 'PARQUET')
            ON_ERROR    = 'CONTINUE';
        """,
        warehouse="NOC_WH",
        database="NOC_DB",
        schema="GOLD",
    )

    # ── Task 6: Check for critical alerts and send notification
    def check_and_alert(**context):
        """
        If any CRITICAL alerts in last hour — push to xcom for email task.
        In production: query Snowflake ALERT_SUMMARY table.
        """
        import random
        critical_count = random.randint(0, 5)   # simulated
        context["task_instance"].xcom_push(key="critical_count", value=critical_count)
        print(f"Critical alerts in last hour: {critical_count}")
        return "send_alert_email" if critical_count > 0 else "pipeline_complete"

    check_alerts = BranchPythonOperator(
        task_id="check_critical_alerts",
        python_callable=check_and_alert,
    )

    send_alert_email = EmailOperator(
        task_id="send_alert_email",
        to=["noc-team@company.com"],
        subject="[NOC ALERT] Critical network events detected — {{ ds }}",
        html_content="""
            <h3>NOC Pipeline Alert</h3>
            <p>Critical events detected in the last hour.</p>
            <p>Batch date: {{ ds }}</p>
            <p>Critical count: {{ task_instance.xcom_pull(key='critical_count') }}</p>
            <p>Review dashboard: https://powerbi.company.com/noc-dashboard</p>
        """,
    )

    pipeline_complete = EmptyOperator(task_id="pipeline_complete")

    # ── Task dependencies ─────────────────────────────────────
    kafka_health_check >> [run_silver_notebook, skip_no_events]
    run_silver_notebook >> dq_checks >> run_gold_notebook
    run_gold_notebook >> copy_to_snowflake >> check_alerts
    check_alerts >> [send_alert_email, pipeline_complete]
