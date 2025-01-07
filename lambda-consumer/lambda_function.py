import json
import base64
from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB Configuration
MONGO_URI = "mongodb+srv://cuongtran:cuongtran123@cluster0.a5r8g.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "main"
COLLECTION_NAME = "weather"

# Create MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def log_message(message):
    """Log messages with a timestamp."""
    current_time_utc = datetime.utcnow()
    current_time_local = current_time_utc + timedelta(hours=7)
    formatted_time = current_time_local.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{formatted_time}] {message}")

def lambda_handler(event, context):
    """Lambda function handler for Kinesis stream events."""
    records_to_insert = []

    for record in event["Records"]:
        try:
            # Decode Base64-encoded Kinesis data
            raw_data = record["kinesis"]["data"]
            payload = base64.b64decode(raw_data).decode("utf-8")
            log_message(f"Raw Payload: {payload}")

            # Parse JSON data
            json_data = json.loads(payload)
            json_data["_id"] = record["kinesis"]["sequenceNumber"]

            # Check for duplicate records
            if collection.find_one({"_id": json_data["_id"]}):
                log_message(f"Record with SequenceNumber {json_data['_id']} already exists. Skipping...")
            else:
                records_to_insert.append(json_data)

        except json.JSONDecodeError as e:
            log_message(f"JSON decoding error: {e}. Skipping record.")
        except Exception as e:
            log_message(f"Error processing record: {e}")

    # Insert non-duplicate records into MongoDB
    if records_to_insert:
        try:
            collection.insert_many(records_to_insert)
            log_message(f"{len(records_to_insert)} records inserted into MongoDB successfully.")
        except Exception as e:
            log_message(f"Error inserting records into MongoDB: {e}")

    return {"statusCode": 200, "body": "Records processed successfully"}
