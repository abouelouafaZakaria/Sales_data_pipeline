from fastapi import FastAPI
from faker import Faker
import random
from datetime import datetime

app = FastAPI(title="Sales Data API")
fake = Faker()

REGIONS = ["Europe", "North America", "Asia", "Africa"]
CATEGORIES = ["Electronics", "Clothing", "Home"]


def noisy(value, field_type):
    if random.random() < 0.2:
        if field_type == "price":
            return -random.uniform(10, 500)
        elif field_type == "quantity":
            return random.choice([-5, 9999])
        elif field_type == "region":
            return "UNKNOWN_REGION"
        elif field_type == "date":
            return "invalid-date"
    return value


@app.get("/")
def root():
    return {"message": "Sales API running"}


@app.get("/products")
def products():
    return [
        {
            "product_id": i,
            "product_name": fake.word(),
            "category": random.choice(CATEGORIES),
            "price": noisy(round(random.uniform(10, 500), 2), "price")
        }
        for i in range(1, 21)
    ]


@app.get("/customers")
def customers():
    return [
        {
            "customer_id": i,
            "name": fake.name(),
            "region": noisy(random.choice(REGIONS), "region")
        }
        for i in range(1, 21)
    ]


@app.get("/orders")
def orders():
    return [
        {
            "order_id": i,
            "customer_id": random.randint(1, 20),
            "product_id": random.randint(1, 20),
            "quantity": noisy(random.randint(1, 10), "quantity"),
            "order_date": noisy(datetime.now().isoformat(), "date")
        }
        for i in range(1, 101)
    ]
