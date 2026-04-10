# ============================================================
# Notebook 2 — Silver Layer: Parse, Enrich, Detect Alerts
# Project: Real-Time Network Event Streaming Pipeline
#
# Reads Bronze Delta → applies business logic → writes Silver
# Runs as a streaming job OR triggered by Airflow DAG
# ============================================================

# ── CELL 1 ── Config
from pyspark.sql import SparkSession, functions as F, Window
import os

LOCAL_DEV   = not os.path.exists("/mnt/delta")
BRONZE_PATH = "/tmp/streaming/bronze"  if LOCAL_DEV else "/mnt/delta/bronze"
SILVER_PATH = "/tmp/streaming/silver"  if LOCAL_DEV else "/mnt/delta/silver"
CHECKPOINT  = "/tmp/streaming/checkpoints/silver" if LOCAL_DEV else "/mnt/checkpoints/silver"

print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")

# ── CELL 2 ── Read Bronze as stream
bronze_stream = (
    spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .load(BRONZE_PATH)
)

# ── CELL 3 ── Flatten payload map → typed columns per event_type
def flatten_payload(df):
    """
    Extract typed fields from the payload MapType column.
    Each event type has different payload keys.
    """
    return (
        df
        # device_health fields
        .withColumn("cpu_utilization_pct",
            F.col("payload")["cpu_utilization_pct"].cast("double"))
        .withColumn("memory_utilization_pct",
            F.col("payload")["memory_utilization_pct"].cast("double"))
        .withColumn("temperature_celsius",
            F.col("payload")["temperature_celsius"].cast("double"))
        .withColumn("uptime_seconds",
            F.col("payload")["uptime_seconds"].cast("long"))
        # interface_stats fields
        .withColumn("interface_name",
            F.col("payload")["interface"])
        .withColumn("packet_loss_pct",
            F.col("payload")["packet_loss_pct"].cast("double"))
        .withColumn("bandwidth_utilization",
            F.col("payload")["bandwidth_utilization"].cast("double"))
        .withColumn("bytes_in",
            F.col("payload")["bytes_in"].cast("long"))
        .withColumn("bytes_out",
            F.col("payload")["bytes_out"].cast("long"))
        # security_alert fields
        .withColumn("alert_type",
            F.col("payload")["alert_type"])
        .withColumn("source_ip",
            F.col("payload")["source_ip"])
        .withColumn("destination_ip",
            F.col("payload")["destination_ip"])
        .withColumn("protocol",
            F.col("payload")["protocol"])
        .withColumn("packet_count",
            F.col("payload")["packet_count"].cast("long"))
        # link_state fields
        .withColumn("link_state",
            F.col("payload")["state"])
        .withColumn("previous_state",
            F.col("payload")["previous_state"])
        .withColumn("link_down_reason",
            F.col("payload")["reason"])
        .drop("payload")
    )

# ── CELL 4 ── Apply enrichment and alert detection
def enrich_and_detect(df):
    return (
        df
        # Severity score for sorting/prioritization
        .withColumn("severity_score",
            F.when(F.col("severity") == "CRITICAL", 4)
             .when(F.col("severity") == "ERROR",    3)
             .when(F.col("severity") == "WARNING",  2)
             .otherwise(1))
        # Is this an actionable alert?
        .withColumn("requires_action",
            (F.col("severity").isin("CRITICAL", "ERROR")) |
            (F.col("event_type") == "security_alert") |
            ((F.col("event_type") == "link_state") & (F.col("link_state") == "down"))
        )
        # Event hour for time-of-day analytics
        .withColumn("event_hour",    F.hour("event_ts"))
        .withColumn("event_date",    F.to_date("event_ts"))
        .withColumn("event_weekday", F.dayofweek("event_ts"))
        # SLA breach flag — CRITICAL events not resolved within threshold
        .withColumn("sla_breach_risk",
            (F.col("severity") == "CRITICAL") &
            (F.col("event_type").isin("device_health", "link_state"))
        )
        # Anomaly flag — high CPU + high memory simultaneously
        .withColumn("resource_anomaly",
            (F.col("cpu_utilization_pct") > 85) &
            (F.col("memory_utilization_pct") > 85)
        )
        # Data center vs branch classification
        .withColumn("location_type",
            F.when(F.col("location").startswith("DC"), "Data Center")
             .otherwise("Branch"))
        .withColumn("_silver_ts", F.current_timestamp())
    )

# ── CELL 5 ── Build Silver stream
silver_stream = (
    bronze_stream
    .transform(flatten_payload)
    .transform(enrich_and_detect)
    .drop("kafka_key", "kafka_topic", "kafka_partition",
          "kafka_offset", "_source")
)

# ── CELL 6 ── Write Silver Delta (partitioned by date + severity)
silver_query = (
    silver_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .partitionBy("event_date", "severity")
    .trigger(processingTime="60 seconds")
    .start(SILVER_PATH)
)

print(f"Silver streaming query: {silver_query.id}")
print(f"Writing to: {SILVER_PATH}")

# ── CELL 7 ── CRITICAL alert micro-batch — write to separate alerts table
ALERTS_PATH = "/tmp/streaming/alerts" if LOCAL_DEV else "/mnt/delta/alerts"
ALERTS_CKPT = "/tmp/streaming/checkpoints/alerts" if LOCAL_DEV else "/mnt/checkpoints/alerts"

alerts_stream = (
    bronze_stream
    .transform(flatten_payload)
    .transform(enrich_and_detect)
    .filter(F.col("requires_action") == True)
)

alerts_query = (
    alerts_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", ALERTS_CKPT)
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")   # faster for alerts
    .start(ALERTS_PATH)
)

print(f"Alerts query: {alerts_query.id} → {ALERTS_PATH}")

# ── CELL 8 ── Audit (static read)
print("\nSilver table sample — CRITICAL events:")
try:
    (
        spark.read.format("delta").load(SILVER_PATH)
        .filter(F.col("severity") == "CRITICAL")
        .select("event_id","event_type","device_id","location",
                "severity","requires_action","event_ts")
        .orderBy(F.col("event_ts").desc())
        .limit(5)
        .show(truncate=False)
    )
except Exception:
    print("Silver table not yet written — run streaming query first.")
