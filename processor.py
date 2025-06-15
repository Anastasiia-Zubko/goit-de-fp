from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, avg, current_timestamp, to_json, struct, broadcast, round as spark_round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import uuid

# Constants
SUFFIX = "zubko"
EVENTS_TOPIC = f'athlete_event_results_{SUFFIX}'
AGG_OUTPUT_TOPIC = f'athlete_enriched_agg_{SUFFIX}'
EVENTS_DBTABLE = "olympic_dataset.athlete_event_results"
ATHLETE_BIO_DBTABLE = "olympic_dataset.athlete_bio"
ATHLETE_AGG_DBTABLE = f"olympic_dataset.athlete_enriched_agg_{SUFFIX}"

# JDBC config
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_properties = {
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Kafka config
kafka_bootstrap_servers = "77.81.230.104:9092"
kafka_user = "admin"
kafka_password = "VawEzo1ikLtrA8Ug8THa"
kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = "PLAIN"
kafka_sasl_jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_user}\" password=\"{kafka_password}\";"

# Spark session
spark = SparkSession.builder \
    .appName("AthleteProcessorHardcoded") \
    .config("spark.jars", "/Users/anastasiiazubko/PycharmProjects/goit-de-fp/mysql-connector-j-9.3.0.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load bio data
bio_df = spark.read.jdbc(
    url=jdbc_url,
    table=ATHLETE_BIO_DBTABLE,
    properties=jdbc_properties
).withColumn("height", col("height").cast(FloatType())) \
 .withColumn("weight", col("weight").cast(FloatType())) \
 .withColumn("athlete_id", col("athlete_id").cast(IntegerType())) \
 .filter(col("height").isNotNull() & col("weight").isNotNull()) \
 .cache()

# Kafka schema
EVENT_SCHEMA = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("timestamp", StringType(), True),
])

# Read stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", EVENTS_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "50000") \
    .option("failOnDataLoss", "false") \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
    .option("kafka.group.id", f"spark-{uuid.uuid4()}") \
    .load()

# Parse Kafka messages
events_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json("json_str", EVENT_SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("athlete_id", col("athlete_id").cast(IntegerType())) \
    .withColumn("ingest_ts", current_timestamp())

# Join and aggregate
joined_df = events_stream.join(broadcast(bio_df), on="athlete_id")

agg_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        spark_round(avg("height"), 3).alias("avg_height"),
        spark_round(avg("weight"), 3).alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

# Write batch
def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty — skipping")
        return

    print(f"Batch {batch_id}: writing {batch_df.count()} rows")

    batch_df.withColumn("value", to_json(struct(*batch_df.columns))) \
        .select("value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", AGG_OUTPUT_TOPIC) \
        .option("kafka.security.protocol", kafka_security_protocol) \
        .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
        .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
        .save()

    batch_df.write.jdbc(url=jdbc_url, table=ATHLETE_AGG_DBTABLE, mode="append", properties=jdbc_properties)

# Start stream
agg_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="15 seconds") \
    .option("checkpointLocation", "./checkpoints/agg_stream") \
    .start() \
    .awaitTermination()
