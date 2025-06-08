from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# 1️⃣ Spark Session
spark = SparkSession.builder \
    .appName("KafkaSparkProductsStreaming") \
    .getOrCreate()

# 2️⃣ Schema → uwzględnia payload.after
json_schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", StringType()),
            StructField("name", StringType()),
            StructField("price", StringType())
        ]))
    ]))
])

# 3️⃣ Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "dbserver1.public.products") \
    .option("startingOffsets", "earliest") \
    .load()

# 4️⃣ Parse payload.after.*
df_parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.payload.after.*")

# 5️⃣ Output
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
