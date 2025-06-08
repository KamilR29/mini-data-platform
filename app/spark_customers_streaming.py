from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# 1️⃣ Tworzymy SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkCustomersStreaming") \
    .getOrCreate()

# 2️⃣ Definiujemy schemat → poprawnie payload.after
json_schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType())
        ]))
    ]))
])

# 3️⃣ Czytamy dane z Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "dbserver1.public.customers") \
    .option("startingOffsets", "earliest") \
    .load()

# 4️⃣ Parsujemy value → payload.after.*
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.payload.after.*")

# 5️⃣ Wyświetlamy na konsolę
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
