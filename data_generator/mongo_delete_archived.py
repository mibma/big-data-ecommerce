from pymongo import MongoClient

# -----------------------------
# CONFIGURATION
# -----------------------------
MONGO_URI = "mongodb://admin:admin123@mongodb:27017"
MONGO_DB = "ecommerce"
ORDERS_COLLECTION = "orders"

# -----------------------------
# CONNECT TO MONGO
# -----------------------------
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
orders_col = db[ORDERS_COLLECTION]

# -----------------------------
# DELETE ARCHIVED RECORDS
# -----------------------------
result = orders_col.delete_many({"archived": True})
print(f"Deleted {result.deleted_count} archived orders from MongoDB.")

client.close()

