import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# -----------------------------
# MongoDB Connection
# -----------------------------
client = MongoClient("mongodb://admin:admin123@localhost:27017/?authSource=admin")
db = client["ecommerce"]

collections = {
    "orders": db["orders"],
    "users": db["users"],
    "products": db["products"],
    "warehouses": db["warehouses"],
    "couriers": db["couriers"],
    "inventory": db["inventory"],
    "orders_archive_summary": db["orders_archive_summary"]  # added archive summary
}

# -----------------------------
# Streamlit Settings
# -----------------------------
st.set_page_config(page_title="E-Commerce Dashboard", layout="wide")
st.title("📊 Real-Time E-Commerce Analytics Dashboard")

REFRESH_RATE = 5
st_autorefresh(interval=REFRESH_RATE * 1000, key="refresh")

# -----------------------------
# Load Mongo Collection
# -----------------------------
def load_collection(col):
    return pd.json_normalize(list(col.find()), sep="_")

@st.cache_data(ttl=REFRESH_RATE)
def fetch_all():
    return {
        "orders": load_collection(collections["orders"]),
        "users": load_collection(collections["users"]),
        "products": load_collection(collections["products"]),
        "couriers": load_collection(collections["couriers"]),
        "warehouses": load_collection(collections["warehouses"]),
        "inventory": load_collection(collections["inventory"]),
        "orders_archive_summary": load_collection(collections["orders_archive_summary"])
    }

data = fetch_all()
df_orders = data["orders"]
df_products = data["products"]
df_couriers = data["couriers"]
df_wh = data["warehouses"]
df_archive_summary = data["orders_archive_summary"]

if df_orders.empty and df_archive_summary.empty:
    st.warning("No orders found!")
    st.stop()

# -----------------------------
# Preprocess Fields
# -----------------------------
# Flatten order fields
df_orders["order_time"] = pd.to_datetime(df_orders["order_order_time"])
df_orders["order_total_amount"] = df_orders["order_total_amount"]

# Extract delivery info safely
df_orders["delivery_status"] = df_orders["delivery_status"]
df_orders["delivery_courier_id"] = df_orders["delivery_courier_id"]

# Flatten order_items
order_items = df_orders.explode("order_items").reset_index(drop=True)
items_df = pd.json_normalize(order_items["order_items"]).reset_index(drop=True)
order_items = pd.concat([order_items, items_df], axis=1)

# Convert product_id safely
order_items["product_id"] = pd.to_numeric(order_items["product_id"], errors="coerce")
order_items = order_items.dropna(subset=["product_id"])
order_items["product_id"] = order_items["product_id"].astype(int)

# Clean join keys
df_products["product_id"] = df_products["product_id"].astype(int)
df_couriers["courier_id"] = df_couriers["courier_id"].astype(int)
df_wh["warehouse_id"] = df_wh["warehouse_id"].astype(int)

# -----------------------------
# Combine Archive Summary for KPIs
# -----------------------------
# Total archived orders and revenue
arch_total_orders = df_archive_summary["total_orders"].sum() if not df_archive_summary.empty else 0
arch_total_value = df_archive_summary["total_value"].sum() if not df_archive_summary.empty else 0

# Live orders totals
live_total_orders = len(df_orders)
live_total_value = df_orders["order_total_amount"].sum() if not df_orders.empty else 0

# Combine totals
total_orders_combined = live_total_orders + arch_total_orders
total_value_combined = live_total_value + arch_total_value
avg_value_combined = total_value_combined / total_orders_combined if total_orders_combined > 0 else 0

# Delivered/Pending only from live orders
delivered_count = len(df_orders[df_orders["delivery_status"] == "Delivered"]) if not df_orders.empty else 0
pending_count = len(df_orders[df_orders["delivery_status"] == "Pending"]) if not df_orders.empty else 0

# -----------------------------
# KPIs
# -----------------------------
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Orders", total_orders_combined)
col2.metric("Total Revenue", f"${total_value_combined:,.2f}")
col3.metric("Avg Order Value", f"${avg_value_combined:,.2f}")
col4.metric("Delivered", delivered_count)
col5.metric("Pending", pending_count)

# -----------------------------
# Payment Type Chart
# -----------------------------
pay = df_orders["order_payment_type"].value_counts().reset_index()
pay.columns = ["Payment Type", "Count"]
fig_pay = px.pie(pay, names="Payment Type", values="Count", title="Payment Type Breakdown")
st.plotly_chart(fig_pay, use_container_width=True)

# -----------------------------
# Top Selling Products
# -----------------------------
merged_items = order_items.merge(df_products, on="product_id", how="left")
top_products = merged_items.groupby("name")["quantity"].sum() \
    .reset_index().sort_values("quantity", ascending=False).head(10)

fig_top = px.bar(top_products, x="name", y="quantity", text="quantity",
                 title="Top 10 Best Selling Products")
st.plotly_chart(fig_top, use_container_width=True)

# -----------------------------
# Delivery Status Over Time
# -----------------------------
# -----------------------------
# Delivery Status Over Time
# -----------------------------
# Ensure order_time is datetime
df_orders["order_time"] = pd.to_datetime(df_orders["order_order_time"])

# Drop rows with missing delivery_status
df_del_time = df_orders.dropna(subset=["delivery_status"])

# Group by hour and delivery_status
delivery_time = df_del_time.groupby(
    [pd.Grouper(key="order_time", freq="1H"), "delivery_status"]
).size().reset_index(name="count")

# Sort by order_time
delivery_time = delivery_time.sort_values("order_time")

# Plot line chart
fig_del = px.line(
    delivery_time,
    x="order_time",
    y="count",
    color="delivery_status",
    markers=True,
    title="Delivery Status Over Time"
)
st.plotly_chart(fig_del, use_container_width=True)

# -----------------------------
# Orders Per Courier
# -----------------------------
df_orders_clean_courier = df_orders.dropna(subset=["delivery_courier_id"]).copy()
df_orders_clean_courier["delivery_courier_id"] = df_orders_clean_courier["delivery_courier_id"].astype(int)

courier_orders = df_orders_clean_courier.groupby("delivery_courier_id").size().reset_index(name="count")
courier_orders = courier_orders.merge(df_couriers, left_on="delivery_courier_id", right_on="courier_id")

fig_courier = px.bar(courier_orders, x="name", y="count", title="Orders by Courier")
st.plotly_chart(fig_courier, use_container_width=True)

# -----------------------------
# Orders Per Warehouse
# -----------------------------
merged_wh = merged_items.merge(df_wh, on="warehouse_id", how="left")
warehouse_orders = merged_wh.groupby("location").size().reset_index(name="count")

fig_wh = px.bar(warehouse_orders, x="location", y="count", title="Orders by Warehouse")
st.plotly_chart(fig_wh, use_container_width=True)
