import boto3
import json
import urllib3
from datetime import datetime

http = urllib3.PoolManager()
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

DRIVER_ID_QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/253613561634/Driver_id_Q'
S3_BUCKET = 'f1-75'
METADATA_KEY = 'metadata/processed_drivers.json'

def read_metadata():
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=METADATA_KEY)
        data = response['Body'].read()
        processed = json.loads(data)
        print(f"📋 Loaded metadata with {len(processed)} driver session keys")
        return processed
    except s3.exceptions.NoSuchKey:
        print("⚠️ Metadata file not found, initializing empty list")
        return []
    except Exception as e:
        print(f"❌ Error reading metadata: {e}")
        return []

def write_metadata(processed):
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=METADATA_KEY,
            Body=json.dumps(processed)
        )
        print(f"✅ Updated metadata with {len(processed)} driver session keys")
    except Exception as e:
        print(f"❌ Error writing metadata: {e}")

def lambda_handler(event, context):
    processed = read_metadata()
    count_sent = 0

    for record in event['Records']:
        body = json.loads(record['body'])
        meeting_key = body.get('meeting_key')
        session_key = body.get('session_key')

        if not meeting_key or not session_key:
            print("❌ Missing meeting_key or session_key in message body.")
            continue

        session_id = f"{meeting_key}_{session_key}"
        if session_id in processed:
            print(f"📂 Drivers for session {session_id} already processed, skipping...")
            continue

        date_today = datetime.utcnow().strftime("%Y-%m-%d")

        # ✅ Fetch and store drivers list
        drivers_url = f"https://api.openf1.org/v1/drivers?session_key={session_key}"
        drivers_resp = http.request('GET', drivers_url)
        if drivers_resp.status == 200:
            drivers_data = json.loads(drivers_resp.data.decode('utf-8'))
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"raw_data/drivers_raw/{session_key}/drivers_{date_today}.json",
                Body=json.dumps(drivers_data)
            )
            print(f"✅ Stored drivers data for session {session_key}")
        else:
            print(f"❌ Failed to fetch drivers for session {session_key}")

        # ✅ Fetch and store weather data
        weather_url = f"https://api.openf1.org/v1/weather?session_key={session_key}"
        weather_resp = http.request('GET', weather_url)
        if weather_resp.status == 200:
            weather_data = json.loads(weather_resp.data.decode('utf-8'))
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"raw_data/weather_raw/{session_key}/weather_{date_today}.json",
                Body=json.dumps(weather_data)
            )
            print(f"✅ Stored weather data for session {session_key}")
        else:
            print(f"❌ Failed to fetch weather for session {session_key}")

        # ✅ Fetch driver numbers from position data and push to Driver_id_Q
        pos_url = f"https://api.openf1.org/v1/position?session_key={session_key}"
        pos_resp = http.request('GET', pos_url)
        if pos_resp.status != 200:
            print(f"❌ Failed to fetch positions for session_key: {session_key}")
            continue

        positions = json.loads(pos_resp.data.decode('utf-8'))
        driver_numbers = list(set([item.get('driver_number') for item in positions if 'driver_number' in item]))

        for driver_number in driver_numbers:
            msg = {
                "session_key": session_key,
                "driver_number": driver_number
            }
            sqs.send_message(
                QueueUrl=DRIVER_ID_QUEUE_URL,
                MessageBody=json.dumps(msg)
            )
            print(f"📤 Sent to Driver_id_Q: session={session_key}, driver={driver_number}")
            count_sent += 1

        processed.append(session_id)

    write_metadata(processed)

    return {
        "statusCode": 200,
        "body": f"✅ Processed {count_sent} drivers and added driver/weather data."
    }

