import math
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from typing import Optional
from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, COLLECTION_NAME

def sanitize(obj):
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize(item) for item in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    else:
        return obj

app = FastAPI(title="Inventory API")

# Connect to MongoDB

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Serve static files
app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/")
def read_root():
    return FileResponse("index.html")

@app.get("/listings")
def get_listings(
    sku: Optional[str] = None,
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    fulfillment_channel: Optional[str] = None
):
    """
    Get active listings from MongoDB with optional filtering.
    - sku: get single item by seller-sku
    - min_price / max_price: price range filter
    - fulfillment_channel: filter by 'DEFAULT' (MFN) or 'AMAZON_FULFILLED' (FBA)
    """
    query = {"status": "active"}

    if sku:
        query["seller-sku"] = sku

    price_query = {}
    if min_price is not None:
        price_query["$gte"] = min_price
    if max_price is not None:
        price_query["$lte"] = max_price
    if price_query:
        query["price"] = price_query

    if fulfillment_channel:
        query["fulfillment-channel"] = fulfillment_channel

    results = list(collection.find(query, {"_id": 0}))

    if sku and not results:
        raise HTTPException(status_code=404, detail=f"SKU {sku} not found")

    return {"count": len(results), "listings": sanitize(results)}
