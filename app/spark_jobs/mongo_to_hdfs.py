from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, current_date,
    sum as _sum, avg as _avg, min as _min, max as _max, count as _count,
    lit
)

# -----------------------------
# CONFIGURATION
# -----------------------------
MONGO_URI = "mongodb://admin:admin123@mongodb:27017"
MONGO_DB = "ecommerce"
MONGO_COLLECTION = "orders"
ARCHIVE_SUMMARY_COLLECTION = "orders_archive_summary"

HDFS_ARCHIVE_PATH = "hdfs://namenode:9000/ecommerce/orders_archive/"
ARCHIVE_DAYS = 1
MAX_SIZE_MB = 5  # Max size before forcing archive

# -----------------------------
# SPARK SESSION
# -----------------------------
spark = SparkSession.builder \
    .appName("MongoToHDFSArchiveJob") \
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

# Add order_date
df_orders = df_orders.withColumn("order_date", to_date(col("order.order_time")))

# Filter rows older than ARCHIVE_DAYS
archive_cutoff = current_date() - ARCHIVE_DAYS
df_old = df_orders.filter(col("order_date") < archive_cutoff)

# Estimate total size in MB
df_size_bytes = df_orders.rdd.map(lambda row: len(str(row))).sum()
df_size_mb = df_size_bytes / (1024 * 1024)

# -----------------------------
# DECIDE WHICH ROWS TO ARCHIVE
# -----------------------------
if df_old.count() > 0 or df_size_mb > MAX_SIZE_MB:

    df_to_archive = df_old if df_old.count() > 0 else df_orders
    count = df_to_archive.count()
    print(f"Archiving {count} records to HDFS... (DataFrame size: {df_size_mb:.2f} MB)")

    # -----------------------------
    # WRITE TO HDFS
    # -----------------------------
    df_to_archive.write \
        .mode("append") \
        .partitionBy("order_date") \
        .parquet(HDFS_ARCHIVE_PATH)

    print(f"Archived to HDFS at: {HDFS_ARCHIVE_PATH}")

    # -----------------------------
    # MARK RECORDS AS ARCHIVED IN MONGO (this is temporary)
    # -----------------------------
    df_to_update = df_to_archive.withColumn("archived", lit(True))
    df_to_update.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", MONGO_DB) \
        .option("collection", MONGO_COLLECTION) \
        .save()

    print(f"Marked {count} records as archived in MongoDB.")

    # -----------------------------
    # CREATE SUMMARY AND APPEND TO NEW COLLECTION
    # -----------------------------
    df_summary = df_to_archive.agg(
        _count("_id").alias("total_orders"),
        _sum("order.total_amount").alias("total_value"),
        _avg("order.total_amount").alias("avg_value"),
        _min("order.total_amount").alias("min_value"),
        _max("order.total_amount").alias("max_value")
    ).withColumn("archive_date", current_date())

    df_summary.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", MONGO_DB) \
        .option("collection", ARCHIVE_SUMMARY_COLLECTION) \
        .save()

    print(f"Appended archive summary to MongoDB collection '{ARCHIVE_SUMMARY_COLLECTION}'.")

    # -------------------------------------------------------------
    # REMOVE ARCHIVED ROWS FROM MAIN COLLECTION (YOUR FIXED VERSION)
    # -------------------------------------------------------------
    # Mark archived rows
    df_to_archive_flagged = df_to_archive.withColumn("archived", lit(True))

    # Mark remaining rows as active
    df_to_keep_flagged = df_orders.join(df_to_archive.select("_id"), "_id", "left_anti") \
                                  .withColumn("archived", lit(False))

    # Combine both datasets
    df_final = df_to_keep_flagged.unionByName(df_to_archive_flagged, allowMissingColumns=True)

    # Overwrite the Mongo collection with final version
    df_final.write \
        .format("mongodb") \
        .mode("overwrite") \
        .option("database", MONGO_DB) \
        .option("collection", MONGO_COLLECTION) \
        .save()

    print(f"Overwritten MongoDB collection '{MONGO_COLLECTION}' with updated archived flags.")

else:
    print(f"No records to archive. (DataFrame size: {df_size_mb:.2f} MB)")

spark.stop()
