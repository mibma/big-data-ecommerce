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
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# -------------------------------
# Kafka Producer
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
# Dimension Tables (STATIC)
# -------------------------------

users = pd.DataFrame([{
    "user_id": i,
    "name": fake.name(),
    "email": fake.email(),
    "location": fake.city(),
    "age": random.randint(18, 70),
    "signup_date": fake.date_this_decade().isoformat()
} for i in range(1, NUM_USERS + 1)])

products = pd.DataFrame([{
    "product_id": i,
    "name": fake.word().capitalize(),
    "category": random.choice(["Electronics", "Clothing", "Home", "Books", "Toys"]),
    "price": round(random.uniform(500, 50000), 2),
    "cost": round(random.uniform(300, 30000), 2),
    "warehouse_id": random.randint(1, NUM_WAREHOUSES)
} for i in range(1, NUM_PRODUCTS + 1)])

warehouses = pd.DataFrame([{
    "warehouse_id": i,
    "location": fake.city(),
    "capacity": random.randint(1000, 5000),
    "manager_id": fake.random_number(digits=4)
} for i in range(1, NUM_WAREHOUSES + 1)])

couriers = pd.DataFrame([{
    "courier_id": i,
    "name": fake.name(),
    "region": fake.city(),
    "rating": round(random.uniform(2.5, 5.0), 1)
} for i in range(1, NUM_COURIERS + 1)])

# -------------------------------
# ID Counters
# -------------------------------
order_id_counter = 1
order_item_id_counter = 1
delivery_id_counter = 1

# -------------------------------
# Order Generator
# -------------------------------
def generate_order():
    global order_id_counter, order_item_id_counter, delivery_id_counter

    user = users.sample(1).iloc[0]
    num_items = random.randint(1, 5)
    order_time = datetime.utcnow()

    order_items = []
    total_amount = 0

    for _ in range(num_items):
        product = products.sample(1).iloc[0]
        quantity = random.randint(1, 3)
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

    discount = round(random.uniform(0, 0.2), 2)
    total_amount = round(total_amount * (1 - discount), 2)

    order = {
        "order_id": order_id_counter,
        "user_id": int(user["user_id"]),
        "order_time": order_time.isoformat(),
        "total_amount": total_amount,
        "discount": discount,
        "quantity": num_items,
        "payment_type": random.choice(["Credit Card", "Cash", "Wallet"])
    }

    courier = couriers.sample(1).iloc[0]
    expected_time = order_time + timedelta(hours=random.randint(6, 72))
    actual_time = expected_time + timedelta(minutes=random.randint(-30, 180))

    delivery = {
        "delivery_id": delivery_id_counter,
        "order_id": order_id_counter,
        "status": random.choice(["Pending", "Shipped", "Delivered"]),
        "expected_time": expected_time.isoformat(),
        "actual_time": actual_time.isoformat(),
        "courier_id": int(courier["courier_id"])
    }

    order_id_counter += 1
    delivery_id_counter += 1

    return order, order_items, delivery

# -------------------------------
# Streaming Loop
# -------------------------------
print("🚀 Faker-based Kafka streaming started...")

while True:
    # Generate order
    order, order_items, delivery = generate_order()

    # Combine into one message
    combined_message = {
        "order": order,
        "order_items": order_items,
        "delivery": delivery
    }

    # Send the combined message to the "orders" topic
    producer.send("orders", combined_message)
    producer.flush()  # ensure it's sent

    print(f"✅ Order {order['order_id']} pushed to Kafka")

    time.sleep(1)

