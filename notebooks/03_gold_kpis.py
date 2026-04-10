# ============================================================
# Notebook 3 — Gold Layer: Aggregations + KPI Tables
# Project: NOC Real-Time Network Event Streaming Pipeline
# Use case: Telecom / Network Operations Center
#
# Reads Silver Delta → builds 4 Gold tables for Snowflake/BI
# Runs as batch (triggered by Airflow every hour)
# ============================================================

# ── CELL 1 ── Config
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType, DateType
)
from datetime import datetime, timedelta
import random, os

LOCAL_DEV   = not os.path.exists("/mnt/delta")
SILVER_PATH = "/tmp/streaming/silver" if LOCAL_DEV else "/mnt/delta/silver"
ALERTS_PATH = "/tmp/streaming/alerts" if LOCAL_DEV else "/mnt/delta/alerts"
GOLD_PATH   = "/tmp/streaming/gold"   if LOCAL_DEV else "/mnt/delta/gold"

print(f"Gold build started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ── CELL 2 ── Synthetic data for local demo (no Kafka needed)
random.seed(42)
devices   = [f"SW-{i:03d}" for i in range(1,11)] + \
            [f"RTR-{i:03d}" for i in range(1,6)] + \
            [f"FW-{i:03d}"  for i in range(1,4)]
locations = ["DC-East","DC-West","Branch-NY","Branch-LA","Branch-CHI"]
evt_types = ["device_health","interface_stats","security_alert","link_state"]
severities= ["INFO","WARNING","ERROR","CRITICAL"]
base_dt   = datetime(2024, 1, 1)

rows = []
for i in range(1000):
    sev = random.choices(severities, weights=[.60,.25,.10,.05])[0]
    etype = random.choice(evt_types)
    loc   = random.choice(locations)
    rows.append((
        f"evt-{i:05d}",
        etype,
        random.choice(devices),
        random.choice(["Switch","Router","Firewall"]),
        loc,
        "Cisco" if random.random() > 0.3 else random.choice(["Juniper","Arista"]),
        "Catalyst 9300",
        sev,
        base_dt + timedelta(hours=random.randint(0, 720)),
        (base_dt + timedelta(hours=random.randint(0, 720))).date(),
        random.randint(0, 23),
        round(random.uniform(10, 98), 2),   # cpu
        round(random.uniform(20, 92), 2),   # memory
        round(random.uniform(30, 84), 2),   # temp
        round(random.uniform(0, 14), 3),    # packet_loss
        round(random.uniform(0, 100), 2),   # bandwidth
        "Data Center" if loc.startswith("DC") else "Branch",
        sev in ("CRITICAL","ERROR") or etype == "security_alert",
        random.choice(["port_scan_detected","auth_failure_spike","ddos_signature_matched",None]),
        random.choice(["TCP","UDP","ICMP",None]),
        random.randint(100, 100000),
        random.choice(["up","down","flapping",None]),
    ))

schema = StructType([
    StructField("event_id",              StringType()),
    StructField("event_type",            StringType()),
    StructField("device_id",             StringType()),
    StructField("device_type",           StringType()),
    StructField("location",              StringType()),
    StructField("vendor",                StringType()),
    StructField("model",                 StringType()),
    StructField("severity",              StringType()),
    StructField("event_ts",              TimestampType()),
    StructField("event_date",            DateType()),
    StructField("event_hour",            IntegerType()),
    StructField("cpu_utilization_pct",   DoubleType()),
    StructField("memory_utilization_pct",DoubleType()),
    StructField("temperature_celsius",   DoubleType()),
    StructField("packet_loss_pct",       DoubleType()),
    StructField("bandwidth_utilization", DoubleType()),
    StructField("location_type",         StringType()),
    StructField("requires_action",       BooleanType()),
    StructField("alert_type",            StringType()),
    StructField("protocol",              StringType()),
    StructField("packet_count",          IntegerType()),
    StructField("link_state",            StringType()),
])

silver = spark.createDataFrame(rows, schema)
alerts = silver.filter(F.col("requires_action") == True)
print(f"Synthetic silver : {silver.count():,} rows")
print(f"Actionable alerts: {alerts.count():,} rows")

# ── CELL 3 ── Gold 1: Device Health KPIs (hourly per device)
print("\nBuilding gold_device_health_kpis...")

gold_device_health = (
    silver
    .filter(F.col("event_type") == "device_health")
    .groupBy("device_id","device_type","location","location_type","event_date","event_hour")
    .agg(
        F.avg("cpu_utilization_pct")    .alias("avg_cpu_pct"),
        F.max("cpu_utilization_pct")    .alias("max_cpu_pct"),
        F.avg("memory_utilization_pct") .alias("avg_memory_pct"),
        F.max("memory_utilization_pct") .alias("max_memory_pct"),
        F.avg("temperature_celsius")    .alias("avg_temp_celsius"),
        F.max("temperature_celsius")    .alias("max_temp_celsius"),
        F.count("event_id")             .alias("sample_count"),
        F.sum(F.col("severity")
               .isin("CRITICAL","ERROR").cast("int")).alias("critical_count"),
    )
    .withColumn("health_score",
        F.greatest(F.lit(0.0),
            F.round(
                100
                - (F.col("avg_cpu_pct") * 0.40)
                - (F.col("avg_memory_pct") * 0.30)
                - ((F.col("avg_temp_celsius") - 30) / 50 * 30),
            1)
        ))
    .withColumn("health_status",
        F.when(F.col("health_score") >= 80, "Healthy")
         .when(F.col("health_score") >= 60, "Degraded")
         .otherwise("Critical"))
    .withColumn("_gold_ts", F.current_timestamp())
)

gold_device_health.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .partitionBy("event_date") \
    .save(f"{GOLD_PATH}/device_health_kpis")
print(f"  gold_device_health_kpis : {gold_device_health.count():,} rows")

# ── CELL 4 ── Gold 2: Alert Summary (daily rollup)
print("\nBuilding gold_alert_summary...")

gold_alerts = (
    alerts
    .groupBy("event_date","event_type","severity","location","location_type","device_type")
    .agg(
        F.count("event_id")          .alias("alert_count"),
        F.countDistinct("device_id") .alias("affected_devices"),
        F.min("event_ts")            .alias("first_alert_ts"),
        F.max("event_ts")            .alias("last_alert_ts"),
    )
    .withColumn("alerts_per_device",
        F.round(F.col("alert_count") / F.col("affected_devices"), 2))
    .withColumn("_gold_ts", F.current_timestamp())
)

gold_alerts.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .partitionBy("event_date") \
    .save(f"{GOLD_PATH}/alert_summary")
print(f"  gold_alert_summary      : {gold_alerts.count():,} rows")

# ── CELL 5 ── Gold 3: Network Reliability (SLA tracking)
print("\nBuilding gold_network_reliability...")

gold_reliability = (
    silver
    .groupBy("location","location_type","device_type","event_date")
    .agg(
        F.count("event_id")                                          .alias("total_events"),
        F.sum(F.col("severity").isin("CRITICAL","ERROR").cast("int")).alias("incident_count"),
        F.avg("packet_loss_pct")                                     .alias("avg_packet_loss"),
        F.max("packet_loss_pct")                                     .alias("max_packet_loss"),
        F.avg("bandwidth_utilization")                               .alias("avg_bandwidth_util"),
        F.countDistinct("device_id")                                 .alias("device_count"),
        F.sum(F.col("requires_action").cast("int"))                  .alias("actionable_alerts"),
    )
    .withColumn("incident_rate_pct",
        F.round(F.col("incident_count") / F.col("total_events") * 100, 2))
    .withColumn("availability_pct",
        F.round(100 - F.col("incident_rate_pct"), 2))
    .withColumn("sla_met", F.col("availability_pct") >= 99.9)
    .withColumn("_gold_ts", F.current_timestamp())
)

gold_reliability.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .partitionBy("event_date") \
    .save(f"{GOLD_PATH}/network_reliability")
print(f"  gold_network_reliability: {gold_reliability.count():,} rows")

# ── CELL 6 ── Gold 4: Security Threat Dashboard
print("\nBuilding gold_security_threats...")

gold_security = (
    silver
    .filter(F.col("event_type") == "security_alert")
    .groupBy("event_date","alert_type","location","device_type","protocol")
    .agg(
        F.count("event_id")          .alias("alert_count"),
        F.sum("packet_count")        .alias("total_packets_flagged"),
        F.countDistinct("device_id") .alias("affected_devices"),
        F.sum(F.col("severity") == "CRITICAL").cast("int").alias("critical_count"),
    )
    .withColumn("threat_level",
        F.when(F.col("critical_count") > 5,  "High")
         .when(F.col("alert_count")    > 10, "Medium")
         .otherwise("Low"))
    .withColumn("_gold_ts", F.current_timestamp())
)

gold_security.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .partitionBy("event_date") \
    .save(f"{GOLD_PATH}/security_threats")
print(f"  gold_security_threats   : {gold_security.count():,} rows")

# ── CELL 7 ── Final audit
print("\n========== GOLD AUDIT ==========")
for name in ["device_health_kpis","alert_summary","network_reliability","security_threats"]:
    try:
        c = spark.read.format("delta").load(f"{GOLD_PATH}/{name}").count()
    except Exception:
        c = 0
    print(f"  gold_{name:<25} {c:>5,} rows")
print("=================================")

# ── CELL 8 ── Sample output
print("\nTop 5 locations by incident count:")
gold_reliability.orderBy(F.col("incident_count").desc()) \
    .select("location","location_type","device_count",
            "incident_count","availability_pct","sla_met") \
    .limit(5).show(truncate=False)
