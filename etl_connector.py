import os
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, ServerSelectionTimeoutError

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
GREYNOISE_RIOT_URL = os.getenv("GREYNOISE_RIOT_URL", "https://viz.greynoise.io/riot")

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    client.server_info()  # Force connection check
    db = client["etl_db"]
    collection = db["greynoise_riot_raw"]
    collection.create_index("ip", unique=True)
    print("Connected to MongoDB.")
except ServerSelectionTimeoutError:
    print("Could not connect to MongoDB. Check your network/VPN and MONGO_URI.")
    exit(1)

def transform(record):
    """Transform a single GreyNoise RIOT record."""
    try:
        ip = record.get("ip", "").strip()
        if not ip:
            return None

        return {
            "ip": ip,
            "name": record.get("name"),
            "category": record.get("category"),
            "description": record.get("description"),
            "last_updated": record.get("last_updated"),
            "ingested_at": datetime.utcnow()
        }
    except Exception as e:
        print(f"Error transforming record {record}: {e}")
        return None

def extract_and_load(batch_size=1000):
    """Fetch GreyNoise RIOT JSON, transform, and load into MongoDB."""
    print("Downloading data from GreyNoise RIOT...")

    retries = 3
    for attempt in range(retries):
        try:
            resp = requests.get(GREYNOISE_RIOT_URL, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            break
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print("Failed to fetch data after multiple attempts.")
                return

    if not isinstance(data, list):
        print("Unexpected API response format.")
        return

    operations = []
    count_inserted = 0
    count_updated = 0
    count_invalid = 0

    for record in data:
        doc = transform(record)
        if not doc:
            count_invalid += 1
            continue

        operations.append(UpdateOne(
            {"ip": doc["ip"]},
            {"$set": doc},
            upsert=True
        ))

        if len(operations) >= batch_size:
            try:
                result = collection.bulk_write(operations, ordered=False)
                count_inserted += result.upserted_count
                count_updated += result.matched_count
            except BulkWriteError as bwe:
                print(f"Bulk write error: {bwe.details}")
            operations = []

    if operations:
        try:
            result = collection.bulk_write(operations, ordered=False)
            count_inserted += result.upserted_count
            count_updated += result.matched_count
        except BulkWriteError as bwe:
            print(f"Bulk write error: {bwe.details}")

    print(f"Pipeline complete: {count_inserted} inserted, {count_updated} updated, {count_invalid} invalid.")

if __name__ == "__main__":
    extract_and_load(batch_size=500)
