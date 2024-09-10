"""
Module to help generate fake data for testing purposes.
"""

import pandas as pd
from faker import Faker

# Initialize Faker
fake = Faker()

# Define the columns
columns = ["id", "name", "email", "address", "phone_number", "date_of_birth"]

# Generate fake data
data = {
    "id": [i + 1 for i in range(100)],
    "name": [fake.name() for _ in range(100)],
    "email": [fake.email() for _ in range(100)],
    "address": [fake.address() for _ in range(100)],
    "phone_number": [fake.phone_number() for _ in range(100)],
    "date_of_birth": [fake.date_of_birth() for _ in range(100)],
}

# Create a DataFrame
df = pd.DataFrame(data, columns=columns)

# Save to CSV
df.to_csv("data.csv", index=False)

# now create some fake purchase data
columns = ["id", "user_id", "product_id", "price", "purchase_date"]

# Generate fake data
fake_price_data = {i + 1: fake.random_int(min=1, max=1000) for i in range(20)}

product_ids = [fake.random_int(min=1, max=20) for _ in range(1000)]

data = {
    "id": [i + 1 for i in range(1000)],
    "user_id": [fake.random_int(min=1, max=100) for _ in range(1000)],
    "product_id": product_ids,
    "price": [fake_price_data[product_id] for product_id in product_ids],
    "purchase_date": [fake.date_this_year() for _ in range(1000)],
}

# Create a DataFrame
df = pd.DataFrame(data, columns=columns)

# Save to SQLLite DB
import sqlite3

conn = sqlite3.connect("purchase_data.db")
df.to_sql("purchase_data", conn, index=False)
df.to_csv("purchase_data.csv", index=False)
conn.close()
