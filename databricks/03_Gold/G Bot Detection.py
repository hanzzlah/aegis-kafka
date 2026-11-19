# Databricks notebook source
from pyspark.sql.functions import col, coalesce, lit, window, count, sum, when, avg, approx_count_distinct, collect_set, explode
from delta.tables import DeltaTable
import requests

# COMMAND ----------

SUPABASE_URL = dbutils.secrets.get(scope="aegis", key="SUPABASE_URL")
SUPABASE_KEY = dbutils.secrets.get(scope="aegis", key="SUPABASE_KEY")

VELOCITY_TABLE = "aegis.gold.account_velocity_alerts"
VELOCITY_CHECKPOINT = "/Volumes/aegis/bronze/checkpoints/gold_account_velocity"

TELEMETRY_SILVER = "aegis.silver.telemetry_interactions"
PROFILES_SILVER = "aegis.silver.user_profiles"
GOLD_TABLE = "aegis.gold.bot_swarm_alerts"
GOLD_CHECKPOINT = "/Volumes/aegis/bronze/checkpoints/gold_bot_swarms"

# COMMAND ----------

telemetry_stream = spark.readStream.table(TELEMETRY_SILVER)
profiles_static = spark.read.table(PROFILES_SILVER).select(
    "user_id", "trust_score", "is_verified", "follower_count"
)

enriched_stream = (telemetry_stream
    .join(profiles_static, on="user_id", how="left")
    .select(
        "interaction_id",
        "user_id",
        "post_id",
        "interaction_type",
        "event_timestamp",
        coalesce(profiles_static["trust_score"], lit(0.5)).alias("trust_score"),
        coalesce(profiles_static["is_verified"], lit(False)).alias("is_verified"),
        coalesce(profiles_static["follower_count"], lit(0)).alias("follower_count")
    )
)

# COMMAND ----------

bot_swarm_alerts = (enriched_stream
    .withWatermark("event_timestamp", "1 minute")
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("post_id")
    )
    .agg(
        count("*").alias("total_interactions"),
        sum(when(col("trust_score") <= 0.5, 1).otherwise(0)).alias("bot_interactions"),
        avg(col("trust_score")).alias("avg_trust_score"),
        approx_count_distinct(col("user_id")).alias("unique_users"),
        collect_set(
            when(col("trust_score") < 0.3, col("user_id"))
        ).alias("bot_user_ids")
    )
    .filter(col("bot_interactions") >= 3)
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "post_id",
        "total_interactions",
        "bot_interactions",
        "avg_trust_score",
        "unique_users",
        "bot_user_ids"
    )
)

account_velocity_alerts = (enriched_stream
    .withWatermark("event_timestamp", "1 minute")
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("user_id")
    )
    .agg(
        count("*").alias("total_interactions"),
        approx_count_distinct(col("post_id")).alias("distinct_posts"),
        avg(col("trust_score")).alias("trust_score"),
        collect_set(col("interaction_type")).alias("interaction_types")
    )
    .filter(col("total_interactions") >= 5)
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "user_id",
        "total_interactions",
        "distinct_posts",
        "trust_score",
        "interaction_types"
    )
)

# COMMAND ----------

def mitigate_bots_in_supabase(user_ids):
    if not user_ids:
        print("ℹ️ No new bot IDs in this batch.", flush=True)
        return
    print(f"🎯 Attempting to mitigate bot IDs: {user_ids}", flush=True)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    # Change "user_id" to "id" if your primary key column in user_profiles is "id"
    params = {
        "user_id": f"in.({','.join(map(str, user_ids))})",
        "trust_score": "gt.0"
    }
    resp = requests.patch(
        f"{SUPABASE_URL}/rest/v1/user_profiles",
        headers=headers,
        params=params,
        json={"trust_score": 0.0},
        timeout=10
    )
    resp.raise_for_status()
    updated = resp.json()
    updated_ids = [row.get("user_id") or row.get("id") for row in updated]
    print(f"🛡️ Updated trust_score to 0.0 for {len(updated_ids)} user(s): {updated_ids}", flush=True)

def upsert_gold_alerts(batch_df, batch_id):
    row_count = batch_df.count()
    print(f"📦 Batch {batch_id} received {row_count} swarm alert(s).", flush=True)
    if row_count == 0:
        return

    if not spark.catalog.tableExists(GOLD_TABLE):
        batch_df.write.format("delta").saveAsTable(GOLD_TABLE)
    else:
        (DeltaTable.forName(spark, GOLD_TABLE).alias("target")
            .merge(
                batch_df.alias("source"),
                "target.window_start = source.window_start AND target.post_id = source.post_id"
            )
            .withSchemaEvolution()
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    bot_rows = batch_df.select(explode("bot_user_ids").alias("uid")).distinct().collect()
    bot_ids = [row["uid"] for row in bot_rows if row["uid"]]
    mitigate_bots_in_supabase(bot_ids)

def upsert_velocity_alerts(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    if not spark.catalog.tableExists(VELOCITY_TABLE):
        batch_df.write.format("delta").saveAsTable(VELOCITY_TABLE)
    else:
        (DeltaTable.forName(spark, VELOCITY_TABLE).alias("target")
            .merge(
                batch_df.alias("source"),
                "target.window_start = source.window_start AND target.user_id = source.user_id"
            )
            .withSchemaEvolution()
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    spammer_ids = [r["user_id"] for r in batch_df.select("user_id").distinct().collect() if r["user_id"]]
    mitigate_bots_in_supabase(spammer_ids)

gold_query = (bot_swarm_alerts.writeStream
    .foreachBatch(upsert_gold_alerts)
    .outputMode("update")
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)

velocity_query = (account_velocity_alerts.writeStream
    .foreachBatch(upsert_velocity_alerts)
    .outputMode("update")
    .option("checkpointLocation", VELOCITY_CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)

gold_query.awaitTermination()
velocity_query.awaitTermination()

# COMMAND ----------

display(
    spark.read.table(GOLD_TABLE)
    .orderBy("window_start", ascending=False)
    .limit(20)
)

# COMMAND ----------

display(
    spark.read.table(VELOCITY_TABLE)
    .orderBy("window_start", ascending=False)
)