# file: generate_dimensions.py
import pandas as pd
import random
from faker import Faker
import os
from datetime import datetime, timedelta

fake = Faker('en_PK')  # Pakistan-centric names, addresses
Faker.seed(42)
random.seed(42)

# Ensure data directory exists
os.makedirs("bigdata-ecommerce/data", exist_ok=True)

# -----------------------------
# 1) USERS
# -----------------------------
pak_cities = ["Karachi", "Lahore", "Islamabad", "Rawalpindi", "Faisalabad", "Multan", "Peshawar", "Quetta"]
users_list = []

for user_id in range(1, 101):  # 100 users
    users_list.append({
        "user_id": user_id,
        "name": fake.name(),
        "email": fake.email(),
        "location": random.choice(pak_cities),
        "age": random.randint(18, 65),
        "signup_date": fake.date_between(start_date='-3y', end_date='today')
    })

users = pd.DataFrame(users_list)
users.to_csv("bigdata-ecommerce/data/users.csv", index=False)

# -----------------------------
# 2) WAREHOUSES
# -----------------------------
warehouses_list = []
for warehouse_id, city in enumerate(pak_cities[:5], start=1):  # 5 warehouses
    warehouses_list.append({
        "warehouse_id": warehouse_id,
        "location": city,
        "capacity": random.randint(5000, 20000),
        "manager_id": random.randint(1, 20)
    })

warehouses = pd.DataFrame(warehouses_list)
warehouses.to_csv("bigdata-ecommerce/data/warehouses.csv", index=False)

# -----------------------------
# 3) PRODUCTS
# -----------------------------
product_categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty"]
products_list = []

for product_id in range(1, 101):  # 100 products
    category = random.choice(product_categories)
    warehouse_id = random.choice(warehouses['warehouse_id'].tolist())
    price = round(random.uniform(100, 20000), 2)
    cost = round(price * random.uniform(0.5, 0.9), 2)
    products_list.append({
        "product_id": product_id,
        "name": fake.word().capitalize() + " " + category[:-1],
        "category": category,
        "price": price,
        "cost": cost,
        "warehouse_id": warehouse_id
    })

products = pd.DataFrame(products_list)
products.to_csv("bigdata-ecommerce/data/products.csv", index=False)

# -----------------------------
# 4) COURIERS
# -----------------------------
regions = ["North", "South", "East", "West", "Central"]
couriers_list = []

for courier_id in range(1, 21):  # 20 couriers
    couriers_list.append({
        "courier_id": courier_id,
        "name": fake.name(),
        "region": random.choice(regions),
        "rating": round(random.uniform(3.0, 5.0), 2)
    })

couriers = pd.DataFrame(couriers_list)
couriers.to_csv("bigdata-ecommerce/data/couriers.csv", index=False)

# -----------------------------
# 5) INVENTORY
# -----------------------------
inventory_list = []
for product_id in products['product_id']:
    warehouse_id = products.loc[products['product_id'] == product_id, 'warehouse_id'].values[0]
    inventory_list.append({
        "product_id": product_id,
        "warehouse_id": warehouse_id,
        "stock_quantity": random.randint(50, 1000),
        "last_restock_time": datetime.now() - timedelta(days=random.randint(0, 30))
    })

inventory = pd.DataFrame(inventory_list)
inventory.to_csv("bigdata-ecommerce/data/inventory.csv", index=False)

print("Dimension tables generated and saved in 'data/' directory!")
