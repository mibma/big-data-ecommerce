from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date
from datetime import timedelta

# -----------------------------
# CONFIGURATION
# -----------------------------
MONGO_URI = "mongodb://admin:admin123@mongodb:27017" 
MONGO_DB = "ecommerce"
MONGO_COLLECTION = "orders"

HDFS_ARCHIVE_PATH = "hdfs://namenode:9000/ecommerce/orders_archive/"
ARCHIVE_DAYS = 1

# -----------------------------
# SPARK SESSION
# -----------------------------
spark = SparkSession.builder \
    .appName("MongoToHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.mongodb.connection.uri", MONGO_URI) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# LOAD DATA FROM MONGODB
# -----------------------------
df_orders = spark.read \
    .format("mongodb") \
    .option("spark.mongodb.connection.uri", MONGO_URI) \
    .option("database", MONGO_DB) \
    .option("collection", MONGO_COLLECTION) \
    .load()

# Convert order.order_time → order_date
df_orders = df_orders.withColumn("order_date", to_date(col("order.order_time")))

# Filter: order_date < (today - ARCHIVE_DAYS)
archive_cutoff = current_date() - ARCHIVE_DAYS
df_to_archive = df_orders.filter(col("order_date") < archive_cutoff)

# -----------------------------
# ARCHIVE TO HDFS
# -----------------------------
if df_to_archive.count() > 0:
    print(f"Archiving {df_to_archive.count()} records to HDFS...")

    df_to_archive.write \
        .mode("append") \
        .partitionBy("order_date") \
        .parquet(HDFS_ARCHIVE_PATH)

    print(f"Archived to HDFS at: {HDFS_ARCHIVE_PATH}")

    # -----------------------------------------
    # DELETE FROM MONGODB USING CONNECTOR 10.x
    # -----------------------------------------
    # deletion requires "_id" field only
    df_ids = df_to_archive.select("_id")

    df_ids.write \
        .format("mongodb") \
        .mode("append") \
        .option("spark.mongodb.connection.uri", MONGO_URI) \
        .option("database", MONGO_DB) \
        .option("collection", MONGO_COLLECTION) \
        .option("operationType", "delete") \
        .save()

    print("Deleted archived records from MongoDB.")

else:
    print("No old data to archive today.")

spark.stop()
