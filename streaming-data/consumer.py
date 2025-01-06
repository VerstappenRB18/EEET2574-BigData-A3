import boto3
import json
import time
from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB Configuration
MONGO_URI = "mongodb+srv://cuongtran:cuongtran123@cluster0.a5r8g.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME = "weather_db"
COLLECTION_NAME = "weather_collection"

# Kinesis Configuration
KINESIS_STREAM_NAME = "weather-data-stream"  # Replace with your Kinesis stream name
AWS_REGION = "us-east-1"

# Create MongoDB client
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Create Kinesis client
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

def log_message(message):
    """Log messages with a timestamp."""
    current_time_utc = datetime.utcnow()
    current_time_local = current_time_utc + timedelta(hours=7)  # Adjust for UTC+7
    formatted_time = current_time_local.strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{formatted_time}] {message}")

def consume_records():
    """Consume records from AWS Kinesis and store them in MongoDB."""
    # Get the shard iterator for the first shard
    response = kinesis_client.describe_stream(StreamName=KINESIS_STREAM_NAME)
    shard_id = response["StreamDescription"]["Shards"][0]["ShardId"]

    shard_iterator_response = kinesis_client.get_shard_iterator(
        StreamName=KINESIS_STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"
    )
    shard_iterator = shard_iterator_response["ShardIterator"]

    log_message(f"Starting to consume records from Kinesis stream: {KINESIS_STREAM_NAME}")

    while True:
        try:
            # Fetch records from Kinesis
            records_response = kinesis_client.get_records(ShardIterator=shard_iterator, Limit=100)
            records = records_response["Records"]

            for record in records:
                data = record["Data"]
                sequence_number = record["SequenceNumber"]

                try:
                    # Parse JSON data
                    json_data = json.loads(data)
                    json_data["_id"] = sequence_number  # Use SequenceNumber as MongoDB's unique ID

                    # Check for duplicate records
                    if collection.find_one({"_id": sequence_number}):
                        log_message(f"Record with SequenceNumber {sequence_number} already exists. Skipping...")
                    else:
                        # Insert the record into MongoDB
                        collection.insert_one(json_data)
                        log_message(f"Record with SequenceNumber {sequence_number} added to MongoDB.")

                except json.JSONDecodeError as e:
                    log_message(f"Error decoding JSON: {e}. Skipping record.")

            # Update shard iterator
            shard_iterator = records_response["NextShardIterator"]

            # Wait before fetching new records to avoid excessive API calls
            time.sleep(2)

        except Exception as e:
            log_message(f"Error consuming records: {e}")

if __name__ == "__main__":
    consume_records()
