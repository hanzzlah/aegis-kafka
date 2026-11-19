# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, to_timestamp, when, current_timestamp


# COMMAND ----------

BRONZE_TABLE = "aegis.bronze.telemetry_interactions"
SILVER_TABLE = "aegis.silver.telemetry_interactions"
SILVER_CHECKPOINT = "/Volumes/aegis/bronze/checkpoints/telemetry_silver"

QUARANTINE_TABLE = "aegis.silver.telemetry_quarantine"
QUARANTINE_CHECKPOINT = "/Volumes/aegis/bronze/checkpoints/silver_telemetry_quarantine"

telemetry_schema = StructType([
    StructField("interaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("post_id", StringType(), False),
    StructField("interaction_type", StringType(), False),
    StructField("timestamp", StringType(), False)
])

# COMMAND ----------

bronze_stream_df = spark.readStream.table(BRONZE_TABLE)

parsed_df = (bronze_stream_df
    .select(
        col("json_payload"),
        from_json(col("json_payload"), telemetry_schema).alias("data"),
        col("kafka_timestamp").alias("ingested_at")
    )
    .select(
        col("json_payload"),
        col("data"),
        col("data.interaction_id").alias("interaction_id"),
        col("data.user_id").alias("user_id"),
        col("data.post_id").alias("post_id"),
        col("data.interaction_type").alias("interaction_type"),
        to_timestamp(col("data.timestamp")).alias("event_timestamp"),
        col("ingested_at")
    )
)

# COMMAND ----------

valid_condition = (
    col("data").isNotNull()
    & col("interaction_id").isNotNull()
    & col("user_id").isNotNull()
    & col("post_id").isNotNull()
    & col("event_timestamp").isNotNull()
)

# COMMAND ----------

silver_df = (parsed_df
    .filter(valid_condition)
    .select("interaction_id", "user_id", "post_id", "interaction_type", "event_timestamp", "ingested_at")
    .withWatermark("event_timestamp", "10 minutes")
    .dropDuplicates(["interaction_id"])
)

quarantine_df = (parsed_df
    .filter(~valid_condition)
    .withColumn(
        "quarantine_reason",
        when(col("data").isNull(), "MALFORMED_JSON")
        .when(col("interaction_id").isNull(), "MISSING_INTERACTION_ID")
        .when(col("user_id").isNull() | col("post_id").isNull(), "MISSING_ENTITY_ID")
        .when(col("event_timestamp").isNull(), "INVALID_TIMESTAMP")
        .otherwise("UNKNOWN_VALIDATION_ERROR")
    )
    .select(
        "json_payload",
        "quarantine_reason",
        "ingested_at",
        current_timestamp().alias("quarantined_at")
    )
)

# COMMAND ----------

silver_query = (silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(availableNow=True)
    .table(SILVER_TABLE)
)

# COMMAND ----------

display(
    spark.read.table(SILVER_TABLE)
    .orderBy("event_timestamp", ascending=False)
)

# COMMAND ----------

quarantine_query = (quarantine_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", QUARANTINE_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(QUARANTINE_TABLE)
)

quarantine_query.awaitTermination()

# COMMAND ----------

display(
    spark.read.table(QUARANTINE_TABLE)
)