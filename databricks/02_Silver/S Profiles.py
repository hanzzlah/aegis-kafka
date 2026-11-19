# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json, coalesce, to_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

BRONZE_TABLE = "aegis.bronze.cdc_profiles"
SILVER_TABLE = "aegis.silver.user_profiles"
SILVER_CHECKPOINT = "/Volumes/aegis/bronze/checkpoints/cdc_profiles_silver"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    user_id STRING,
    created_at TIMESTAMP,
    is_verified BOOLEAN,
    follower_count INT,
    trust_score DOUBLE,
    updated_at TIMESTAMP
) USING DELTA
""")

# COMMAND ----------

row_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("is_verified", BooleanType(), True),
    StructField("follower_count", IntegerType(), True),
    StructField("trust_score", DoubleType(), True),
    StructField("updated_at", StringType(), True)
])

webhook_schema = StructType([
    StructField("type", StringType(), True),
    StructField("record", row_schema, True),
    StructField("old_record", row_schema, True)
])

# COMMAND ----------

def upsert_profiles(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    parsed = (batch_df
        .select(from_json(col("json_payload"), webhook_schema).alias("w"), col("kafka_timestamp"))
        .select(
            col("w.type").alias("op_type"),
            coalesce(col("w.record"), col("w.old_record")).alias("row"),
            col("kafka_timestamp")
        )
        .select(
            "op_type", "kafka_timestamp",
            col("row.user_id"),
            to_timestamp(col("row.created_at")).alias("created_at"),
            col("row.is_verified"),
            col("row.follower_count"),
            col("row.trust_score"),
            to_timestamp(col("row.updated_at")).alias("updated_at")
        )
        .filter(col("user_id").isNotNull())
    )

    latest_window = Window.partitionBy("user_id").orderBy(col("kafka_timestamp").desc())
    deduped = (parsed
        .select("*", row_number().over(latest_window).alias("rn"))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    target = DeltaTable.forName(batch_df.sparkSession, SILVER_TABLE)
    (target.alias("target")
        .merge(deduped.alias("source"), "target.user_id = source.user_id")
        .whenMatchedDelete(condition="source.op_type = 'DELETE'")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll(condition="source.op_type != 'DELETE'")
        .execute()
    )

# COMMAND ----------

(spark.readStream.table(BRONZE_TABLE)
    .writeStream
    .foreachBatch(upsert_profiles)
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

display(spark.read.table(SILVER_TABLE))