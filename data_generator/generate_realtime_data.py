import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import time
import json
from kafka import KafkaProducer

# -------------------------------
# Faker + Seeds
# -------------------------------
fake = Faker()
np.random.seed(42)

# -------------------------------
# Kafka Producer (UNCHANGED)
# -------------------------------
producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

# -------------------------------
# Constants
# -------------------------------
NUM_USERS = 100
NUM_PRODUCTS = 50
NUM_WAREHOUSES = 5
NUM_COURIERS = 10

# -------------------------------
# Dimension Tables (STATISTICAL)
# -------------------------------

users = pd.DataFrame([{
    "user_id": i,
    "name": fake.name(),
    "email": fake.email(),
    "location": fake.city(),
    "age": int(np.clip(np.random.normal(35, 10), 18, 70)),  # Normal distribution
    "signup_date": fake.date_this_decade().isoformat()
} for i in range(1, NUM_USERS + 1)])

products = pd.DataFrame([{
    "product_id": i,
    "name": fake.word().capitalize(),
    "category": np.random.choice(
        ["Electronics", "Clothing", "Home", "Books", "Toys"],
        p=[0.30, 0.25, 0.20, 0.15, 0.10]
    ),
    "price": round(np.random.lognormal(mean=8.5, sigma=0.6), 2),  # realistic pricing
    "cost": round(np.random.lognormal(mean=8.2, sigma=0.5), 2),
    "warehouse_id": np.random.randint(1, NUM_WAREHOUSES + 1)
} for i in range(1, NUM_PRODUCTS + 1)])

warehouses = pd.DataFrame([{
    "warehouse_id": i,
    "location": fake.city(),
    "capacity": int(np.random.normal(3000, 800)),
    "manager_id": fake.random_number(digits=4)
} for i in range(1, NUM_WAREHOUSES + 1)])

couriers = pd.DataFrame([{
    "courier_id": i,
    "name": fake.name(),
    "region": fake.city(),
    "rating": round(np.random.normal(4.2, 0.4), 1)
} for i in range(1, NUM_COURIERS + 1)])

# -------------------------------
# Counters
# -------------------------------
order_id_counter = 1
order_item_id_counter = 1
delivery_id_counter = 1

# -------------------------------
# Order Generator (STATISTICAL)
# -------------------------------
def generate_order():
    global order_id_counter, order_item_id_counter, delivery_id_counter

    user = users.sample(1).iloc[0]
    order_time = datetime.utcnow()

    num_items = max(1, np.random.poisson(2))  # Most orders 1–3 items

    order_items = []
    total_amount = 0

    for _ in range(num_items):
        product = products.sample(1).iloc[0]
        quantity = max(1, np.random.poisson(1.5))
        price = product["price"]

        total_amount += price * quantity

        order_items.append({
            "order_item_id": order_item_id_counter,
            "order_id": order_id_counter,
            "product_id": int(product["product_id"]),
            "quantity": quantity,
            "price": float(price),
            "event_time": order_time.isoformat()
        })

        order_item_id_counter += 1

    # Discount → mostly small
    discount = round(np.random.beta(2, 8), 2)
    total_amount = round(total_amount * (1 - discount), 2)

    order = {
        "order_id": order_id_counter,
        "user_id": int(user["user_id"]),
        "order_time": order_time.isoformat(),
        "total_amount": total_amount,
        "discount": discount,
        "quantity": num_items,
        "payment_type": np.random.choice(
            ["Credit Card", "Wallet", "Cash"],
            p=[0.55, 0.30, 0.15]
        )
    }

    courier = couriers.sample(1).iloc[0]

    expected_time = order_time + timedelta(
        hours=int(np.random.normal(36, 12))
    )

    actual_time = expected_time + timedelta(
        minutes=int(np.random.normal(15, 30))
    )

    delivery = {
        "delivery_id": delivery_id_counter,
        "order_id": order_id_counter,
        "status": np.random.choice(
            ["Pending", "Shipped", "Delivered"],
            p=[0.2, 0.3, 0.5]
        ),
        "expected_time": expected_time.isoformat(),
        "actual_time": actual_time.isoformat(),
        "courier_id": int(courier["courier_id"])
    }

    order_id_counter += 1
    delivery_id_counter += 1

    return order, order_items, delivery

# -------------------------------
# Streaming Loop (UNCHANGED)
# -------------------------------
print("🚀 Kafka streaming with statistical data started...")

while True:
    order, order_items, delivery = generate_order()

    producer.send("orders", {
        "order": order,
        "order_items": order_items,
        "delivery": delivery
    })

    producer.flush()
    print(f"✅ Order {order['order_id']} sent to Kafka ")

    time.sleep(1)
