import re
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Spark session
spark = SparkSession.builder.appName("BronzeToSilverLayer").getOrCreate()

# Clean text function
def clean_text(text):
    return re.sub(r"[^a-zA-Z0-9,.\\\"\' ]", '', str(text))

# UDF
clean_text_udf = udf(clean_text, StringType())

# Create silver dir
Path("silver").mkdir(parents=True, exist_ok=True)

# Read bronze data
df_bio = spark.read.parquet("bronze/athlete_bio")
df_results = spark.read.parquet("bronze/athlete_event_results")

# Clean text columns
df_bio_cleaned = df_bio.withColumn("name", clean_text_udf(df_bio["name"]))
df_results_cleaned = df_results.withColumn("event", clean_text_udf(df_results["event"]))

# Write to silver
df_bio_cleaned.write.mode("overwrite").parquet("silver/athlete_bio")
df_results_cleaned.write.mode("overwrite").parquet("silver/athlete_event_results")

# Show small sample
df_bio_cleaned.show(3)
df_results_cleaned.show(3)

# Print row counts
print(f"Bio rows: {df_bio_cleaned.count()}")
print(f"Results rows: {df_results_cleaned.count()}")

spark.stop()
