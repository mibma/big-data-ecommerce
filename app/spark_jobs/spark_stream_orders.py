from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

# -----------------------------
# Kafka → Spark Streaming Schema
# -----------------------------
order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("order_time", StringType()),
    StructField("total_amount", FloatType()),
    StructField("discount", FloatType()),
    StructField("quantity", IntegerType()),
    StructField("payment_type", StringType())
])

order_items_schema = ArrayType(StructType([
    StructField("order_item_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("price", FloatType())
]))

delivery_schema = StructType([
    StructField("delivery_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("status", StringType()),
    StructField("expected_time", StringType()),
    StructField("actual_time", StringType()),
    StructField("courier_id", IntegerType())
])

full_schema = StructType([
    StructField("order", order_schema),
    StructField("order_items", order_items_schema),
    StructField("delivery", delivery_schema)
])

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("KafkaOrdersStream") \
    .config(
        "spark.mongodb.connection.uri",
        "mongodb://admin:admin123@mongodb:27017/ecommerce.orders?authSource=admin"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# -----------------------------
# Read Kafka Stream
# -----------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING) as json") \
           .select(from_json(col("json"), full_schema).alias("data")) \
           .select("data.*")

df.printSchema()
# -----------------------------
# Write to MongoDB (MongoDB Connector 10.x)
# -----------------------------
query = df.writeStream \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", "mongodb://admin:admin123@mongodb:27017/admin") \
    .option("database", "ecommerce") \
    .option("collection", "orders") \
    .option("checkpointLocation", "/tmp/checkpoint_orders") \
    .outputMode("append") \
    .start()

query.awaitTermination()
