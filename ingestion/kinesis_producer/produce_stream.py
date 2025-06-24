import boto3
import json
import random
import time
from datetime import datetime

# AWS Kinesis stream configuration
STREAM_NAME = "fraud-detection-stream"
REGION_NAME = "us-east-1"

# Initialize the Kinesis client
kinesis_client = boto3.client('kinesis', region_name=REGION_NAME)

# Sample user & transaction data
users = ["user_1001", "user_1002", "user_1003", "user_1004"]
locations = ["NY", "CA", "TX", "FL", "NV"]

def generate_transaction():
    return {
        "transaction_id": f"txn_{random.randint(10000, 99999)}",
        "user_id": random.choice(users),
        "amount": round(random.uniform(10, 5000), 2),
        "location": random.choice(locations),
        "timestamp": datetime.utcnow().isoformat(),
        "is_fraud": random.choice([0, 0, 0, 1])  # Mostly 0, rare 1
    }

def send_data():
    while True:
        record = generate_transaction()
        print(f"Sending: {record}")
        
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(record),
            PartitionKey=record["user_id"]
        )
        print(f"Response: {response['ResponseMetadata']['HTTPStatusCode']}")
        
        time.sleep(2)  # 2 seconds delay to simulate stream

if __name__ == "__main__":
    send_data()
