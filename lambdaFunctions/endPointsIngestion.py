import boto3
import json
import urllib3
from datetime import datetime

http = urllib3.PoolManager()
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

S3_BUCKET = 'f1-75'
TRANSFORM_QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/253613561634/Transform_Q'

ENDPOINTS = [
    "car_data", "intervals", "position", "pit",
    "race_control", "laps", "stints",
    "location", "team_radio"
]

METADATA_KEY = 'metadata/processed_ingestion.json'

def load_metadata():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=METADATA_KEY)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return {}
        else:
            raise

def save_metadata(metadata):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=METADATA_KEY,
        Body=json.dumps(metadata)
    )

def lambda_handler(event, context):
    metadata = load_metadata()
    processed = metadata.get("ingested", [])
    new_entries = []

    for record in event['Records']:
        body = json.loads(record['body'])
        session_key = body.get('session_key')
        driver_number = body.get('driver_number')

        if not session_key or not driver_number:
            print(f"❌ Missing session_key or driver_number: {body}")
            continue

        for endpoint in ENDPOINTS:
            key_triplet = f"{session_key}_{driver_number}_{endpoint}"
            if key_triplet in processed:
                print(f"⚠️ Already processed {key_triplet}, skipping.")
                continue

            url = f"https://api.openf1.org/v1/{endpoint}?session_key={session_key}&driver_number={driver_number}"
            print(f"📡 Fetching {endpoint} for session={session_key}, driver={driver_number}")
            response = http.request('GET', url)

            if response.status != 200:
                print(f"❌ Failed to fetch {endpoint}. Status: {response.status}")
                continue

            data = json.loads(response.data.decode('utf-8'))
            date_today = datetime.utcnow().strftime("%Y-%m-%d")

            s3_key = f"raw_data/{endpoint}_raw/{session_key}/{driver_number}/{endpoint}_{date_today}.json"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(data)
            )
            print(f"✅ Stored {endpoint} at {s3_key}")

            new_entries.append(key_triplet)

    # 🔁 Save updated metadata
    processed.extend(new_entries)
    metadata['ingested'] = processed
    save_metadata(metadata)
    print(f"📝 Updated metadata with {len(new_entries)} new entries.")

    # 📤 Send message to Transformation_Q (only once per batch)
    transformation_message = {
        "meetings_raw": True,
        "sessions_raw": True,
        "drivers_raw": True,
        "laps_raw": True
    }

    sqs.send_message(
        QueueUrl=TRANSFORM_QUEUE_URL,
        MessageBody=json.dumps(transformation_message)
    )
    print(f"📤 Sent transformation trigger to SQS: {transformation_message}")

    return {
        "statusCode": 200,
        "body": f"✅ Processed {len(new_entries)} new (session, driver, endpoint) items"
    }

