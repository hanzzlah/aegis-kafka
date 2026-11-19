# Databricks notebook source
AIVEN_KAFKA_URI = "aegis-hanzzlah.c.aivencloud.com:12632"
CERT_VOLUME_PATH = "/Volumes/aegis/bronze/kafka_certs"
CHECKPOINT_PATH = "/Volumes/aegis/bronze/checkpoints/cdc_profiles_bronze"
TARGET_TABLE = "aegis.bronze.cdc_profiles"

with open(f"{CERT_VOLUME_PATH}/ca.pem", "r") as f: ca_cert = f.read()
with open(f"{CERT_VOLUME_PATH}/service.cert", "r") as f: service_cert = f.read()
with open(f"{CERT_VOLUME_PATH}/service.key", "r") as f: service_key = f.read()

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": AIVEN_KAFKA_URI,
    "subscribe": "cdc_profiles",
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SSL",
    "kafka.ssl.truststore.type": "PEM",
    "kafka.ssl.truststore.certificates": ca_cert,
    "kafka.ssl.keystore.type": "PEM",
    "kafka.ssl.keystore.certificate.chain": service_cert,
    "kafka.ssl.keystore.key": service_key
}

raw_cdc_df = spark.readStream.format("kafka").options(**kafka_options).load()

# COMMAND ----------

(raw_cdc_df
    .selectExpr("CAST(value AS STRING) AS json_payload", "timestamp AS kafka_timestamp")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .table(TARGET_TABLE)
)