# ============================================================
# Notebook 1 — Spark Structured Streaming: Kafka → Bronze Delta
# Project: Real-Time Network Event Streaming Pipeline
# Mirrors: Cisco Systems experience — Kafka + Spark Streaming
#
# Run on: Databricks (attach to cluster with kafka library)
# Or locally with: pip install pyspark delta-spark kafka-python
# ============================================================

# ── CELL 1 ── Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, LongType, MapType
)
import os

LOCAL_DEV    = not os.path.exists("/mnt/delta")
KAFKA_BROKER = "localhost:9092" if LOCAL_DEV else "kafka-broker:9092"
KAFKA_TOPIC  = "network-events"
BRONZE_PATH  = "/tmp/streaming/bronze" if LOCAL_DEV else "/mnt/delta/bronze"
CHECKPOINT   = "/tmp/streaming/checkpoints/bronze" if LOCAL_DEV else "/mnt/checkpoints/bronze"

print(f"Kafka broker : {KAFKA_BROKER}")
print(f"Topic        : {KAFKA_TOPIC}")
print(f"Bronze path  : {BRONZE_PATH}")

# ── CELL 2 ── Define event schema (matches kafka_producer.py output)
event_schema = StructType([
    StructField("event_id",    StringType(),    True),
    StructField("event_type",  StringType(),    True),
    StructField("device_id",   StringType(),    True),
    StructField("device_type", StringType(),    True),
    StructField("location",    StringType(),    True),
    StructField("vendor",      StringType(),    True),
    StructField("model",       StringType(),    True),
    StructField("severity",    StringType(),    True),
    StructField("timestamp",   StringType(),    True),
    StructField("payload",     MapType(StringType(), StringType()), True),
])

# ── CELL 3 ── Read from Kafka (Structured Streaming)
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 1000)    # micro-batch size
    .option("failOnDataLoss", "false")
    .load()
)

print("Kafka stream connected. Schema:")
raw_stream.printSchema()

# ── CELL 4 ── Parse JSON payload and add Bronze metadata
parsed_stream = (
    raw_stream
    .select(
        F.col("key").cast("string").alias("kafka_key"),
        F.from_json(F.col("value").cast("string"), event_schema).alias("event"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .select(
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "event.*",
    )
    # Add Bronze metadata
    .withColumn("_ingest_ts",    F.current_timestamp())
    .withColumn("_batch_date",   F.current_date())
    .withColumn("_source",       F.lit("kafka"))
    .withColumn("event_ts",      F.to_timestamp("timestamp"))
)

# ── CELL 5 ── Write to Bronze Delta Lake (partitioned by date + event_type)
bronze_query = (
    parsed_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .partitionBy("_batch_date", "event_type")
    .trigger(processingTime="30 seconds")   # micro-batch every 30s
    .start(BRONZE_PATH)
)

print(f"\nStreaming query started: {bronze_query.id}")
print(f"Writing to Bronze Delta: {BRONZE_PATH}")
print("Status: ", bronze_query.status)

# ── CELL 6 ── Monitor stream (run in separate cell)
# bronze_query.awaitTermination()   # blocks until stopped
# bronze_query.stop()               # stop the stream

# Check latest micro-batch stats
# bronze_query.lastProgress

# ── CELL 7 ── Verify Bronze data (static read for audit)
print("\nBronze Delta table — latest records:")
(
    spark.read.format("delta").load(BRONZE_PATH)
    .orderBy(F.col("_ingest_ts").desc())
    .select("event_id","event_type","device_id","severity","event_ts","_ingest_ts")
    .limit(10)
    .show(truncate=False)
)
