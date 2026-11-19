# Databricks notebook source
# Define Core Variables and Unity Catalog Paths
AIVEN_KAFKA_URI = "aegis-hanzzlah.c.aivencloud.com:12632"
CATALOG_NAME = "aegis"
SCHEMA_NAME = "bronze"

CERT_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/kafka_certs"
CHECKPOINT_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/checkpoints/telemetry_bronze"
TARGET_TABLE = f"{CATALOG_NAME}.{SCHEMA_NAME}.telemetry_interactions"

# Read the certificate contents directly into memory as strings
with open(f"{CERT_VOLUME_PATH}/ca.pem", "r") as f:
    ca_cert = f.read()

with open(f"{CERT_VOLUME_PATH}/service.cert", "r") as f:
    service_cert = f.read()

with open(f"{CERT_VOLUME_PATH}/service.key", "r") as f:
    service_key = f.read()

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": AIVEN_KAFKA_URI,
    "subscribe": "telemetry_interactions",
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.type": "PEM",
    "kafka.ssl.truststore.certificates": ca_cert,
    "kafka.ssl.keystore.type": "PEM",
    "kafka.ssl.keystore.certificate.chain": service_cert,
    "kafka.ssl.keystore.key": service_key
}

# COMMAND ----------

# Ingest Raw Byte Stream from Aiven Kafka
raw_kafka_df = (spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# COMMAND ----------

# Cast Binary to String and Sink to Bronze Delta Table
bronze_streaming_query = (raw_kafka_df
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key", 
        "CAST(value AS STRING) AS json_payload", 
        "timestamp AS kafka_timestamp", 
        "topic"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True) # Forces it to run as a micro-batch
    .table(TARGET_TABLE)
)

# COMMAND ----------

display(
    spark.read.table(TARGET_TABLE)
    .orderBy("kafka_timestamp", ascending=False)
)