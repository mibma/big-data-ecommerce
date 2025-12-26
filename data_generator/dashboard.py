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
    "inventory": db["inventory"]
}

# -----------------------------
# Streamlit Settings
# -----------------------------
st.set_page_config(page_title="E-Commerce Dashboard", layout="wide")
st.title("📊 Real-Time E-Commerce Analytics Dashboard")

REFRESH_RATE = 5
st_autorefresh(interval=REFRESH_RATE * 1000, key="refresh")

# -----------------------------
# Helper: Load Collection
# -----------------------------
def load_collection(col):
    df = pd.json_normalize(list(col.find()), sep="_")
    return df

@st.cache_data(ttl=REFRESH_RATE)
def fetch_all():
    return {
        "orders": load_collection(collections["orders"]),
        "users": load_collection(collections["users"]),
        "products": load_collection(collections["products"]),
        "couriers": load_collection(collections["couriers"]),
        "warehouses": load_collection(collections["warehouses"]),
        "inventory": load_collection(collections["inventory"])
    }

data = fetch_all()
df_orders = data["orders"]

if df_orders.empty:
    st.warning("No orders found yet!")
    st.stop()

df_products = data["products"]

# -----------------------------
# Preprocess Data
# -----------------------------
# Convert order time
if "order_order_time" not in df_orders.columns:
    st.error("❌ Missing 'order_order_time'. Your Mongo documents are not being flattened.")
    st.write(df_orders.head())
    st.stop()

df_orders["order_time"] = pd.to_datetime(df_orders["order_order_time"])

# Ensure IDs are strings to merge safely
df_products["product_id_str"] = df_products["_id"].astype(str) if "_id" in df_products.columns else df_products["product_id"].astype(str)

# -----------------------------
# KPIs
# -----------------------------
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Orders", len(df_orders))
col2.metric("Total Revenue", f"${df_orders['order_total_amount'].sum():,.2f}")
col3.metric("Avg Order Value", f"${df_orders['order_total_amount'].mean():,.2f}")
col4.metric("Delivered Orders", len(df_orders[df_orders["delivery_status"] == "Delivered"]))
col5.metric("Pending Orders", len(df_orders[df_orders["delivery_status"] == "Pending"]))

# -----------------------------
# Orders by Payment Type
# -----------------------------
if "order_payment_type" in df_orders.columns:
    pay = df_orders["order_payment_type"].value_counts().reset_index()
    pay.columns = ["Payment Type", "Count"]
    fig_pay = px.pie(pay, names="Payment Type", values="Count", title="Payment Breakdown")
    st.plotly_chart(fig_pay, use_container_width=True)

# -----------------------------
# Top Products Sold
# -----------------------------
if "order_items" in df_orders.columns:
    exploded = df_orders.explode("order_items")
    items_df = pd.json_normalize(exploded["order_items"])
    items_df["product_id"] = items_df["product_id"].astype(str)

    df_products_renamed = df_products.rename(columns={"name": "product_name"})
    merged = pd.merge(
        items_df,
        df_products_renamed,
        left_on="product_id",
        right_on="product_id_str",
        how="left"
    ).dropna(subset=["product_name"])

    if not merged.empty:
        top_products = merged.groupby("product_name")["quantity"].sum().reset_index().sort_values("quantity", ascending=False).head(10)
        fig_top = px.bar(top_products, x="product_name", y="quantity", text="quantity",
                         title="Top 10 Products Sold")
        fig_top.update_traces(marker_color="indianred", textposition="outside")
        st.plotly_chart(fig_top, use_container_width=True)

# -----------------------------
# Delivery Status Over Time
# -----------------------------
delivery = df_orders.groupby([pd.Grouper(key="order_time", freq="30min"), "delivery_status"]).size().reset_index(name="count")
fig_delivery = px.line(delivery, x="order_time", y="count", color="delivery_status", title="📦 Delivery Status Over Time")
st.plotly_chart(fig_delivery, use_container_width=True)

# -----------------------------
# Orders per Courier
# -----------------------------
if "delivery_courier_id" in df_orders.columns:
    courier_orders = df_orders.groupby("delivery_courier_id").size().reset_index(name="count")
    
    # Make types consistent
    courier_orders["delivery_courier_id"] = courier_orders["delivery_courier_id"].astype(str)
    df_couriers = data["couriers"].copy()
    df_couriers["_id"] = df_couriers["_id"].astype(str)
    
    courier_orders = courier_orders.merge(df_couriers, left_on="delivery_courier_id", right_on="_id", how="left")
    
    fig_courier = px.bar(courier_orders, x="name", y="count", title="Orders per Courier")
    st.plotly_chart(fig_courier, use_container_width=True)

# -----------------------------
# Orders per Warehouse
# -----------------------------
if "order_items" in df_orders.columns:
    exploded = df_orders.explode("order_items")
    items_df = pd.json_normalize(exploded["order_items"])
    items_df["product_id"] = items_df["product_id"].astype(str)
    df_products_renamed = df_products.rename(columns={"name": "product_name"})
    merged_wh = pd.merge(items_df, df_products_renamed, left_on="product_id", right_on="product_id_str", how="left")
    
    warehouse_orders = merged_wh.groupby("warehouse_id").size().reset_index(name="count")
    
    # Make types consistent
    warehouse_orders["warehouse_id"] = warehouse_orders["warehouse_id"].astype(str)
    df_warehouses = data["warehouses"].copy()
    df_warehouses["_id"] = df_warehouses["_id"].astype(str)
    
    warehouse_orders = warehouse_orders.merge(df_warehouses, left_on="warehouse_id", right_on="_id", how="left")
    
    fig_wh = px.bar(warehouse_orders, x="name", y="count", title="Orders per Warehouse")
    st.plotly_chart(fig_wh, use_container_width=True)
