from pyspark.sql import SparkSession

# MySQL configs
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
jdbc_table = "olympic_dataset.athlete_event_results"

# Kafka configs
kafka_bootstrap_servers = "77.81.230.104:9092"
kafka_user = "admin"
kafka_password = "VawEzo1ikLtrA8Ug8THa"
kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = "PLAIN"
kafka_sasl_jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_user}\" password=\"{kafka_password}\";"

topic = "athlete_event_results_zubko"

# Spark session
spark = SparkSession.builder \
    .appName("AthleteEventResultsProducer") \
    .config("spark.jars", "/Users/anastasiiazubko/PycharmProjects/goit-de-fp/mysql-connector-j-9.3.0.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read MySQL
df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver=jdbc_driver,
    dbtable=jdbc_table,
    user=jdbc_user,
    password=jdbc_password
).load()

# Write to Kafka
df.selectExpr("CAST(athlete_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", kafka_sasl_jaas_config) \
    .option("topic", topic) \
    .save()

print("Successfully loaded data to Kafka topic")
