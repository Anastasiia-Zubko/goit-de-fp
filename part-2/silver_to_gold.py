from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
from pathlib import Path

# Spark session
spark = SparkSession.builder.appName("SilverToGoldLayer").getOrCreate()

# Create gold folder
Path("gold").mkdir(parents=True, exist_ok=True)

# Read silver data
df_bio = spark.read.parquet("silver/athlete_bio")
df_results = spark.read.parquet("silver/athlete_event_results")

# Join on athlete_id
df_joined = df_results.join(df_bio, on="athlete_id", how="inner")

# Aggregate averages
df_avg = df_joined.groupBy(
    "sport",
    "medal",
    "sex",
    df_bio["country_noc"]
).agg(
    avg("weight").alias("avg_weight"),
    avg("height").alias("avg_height")
).withColumn(
    "timestamp", current_timestamp()
)

# Show result
df_avg.show()

# Save to gold
df_avg.write.mode("overwrite").parquet("gold/avg_stats")

# Stop Spark
spark.stop()