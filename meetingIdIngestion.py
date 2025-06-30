
import boto3
import json
import urllib3

sqs = boto3.client('sqs')
s3 = boto3.client('s3')
http = urllib3.PoolManager()

QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/253613561634/Meeting_id_Q'
S3_BUCKET = 'f1-75'
S3_FOLDER = 'raw_data/meetings_raw/'
METADATA_KEY = 'metadata/processed_meetings.json'

def read_metadata():
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=METADATA_KEY)
        data = response['Body'].read()
        processed_meetings = json.loads(data)
        print(f"📋 Loaded metadata with {len(processed_meetings)} meetings")
        return processed_meetings
    except s3.exceptions.NoSuchKey:
        print("⚠️ Metadata file not found, initializing empty list")
        return []
    except Exception as e:
        print(f"❌ Error reading metadata: {e}")
        return []

def write_metadata(processed_meetings):
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=METADATA_KEY,
            Body=json.dumps(processed_meetings)
        )
        print(f"✅ Updated metadata with {len(processed_meetings)} meetings")
    except Exception as e:
        print(f"❌ Error writing metadata: {e}")

def lambda_handler(event, context):
    url = 'https://api.openf1.org/v1/meetings'
    response = http.request('GET', url)

    if response.status != 200:
        raise Exception("Failed to fetch meetings")

    meetings = json.loads(response.data.decode('utf-8'))
    meetings_2025 = [m for m in meetings if m.get('date_start', '').startswith("2025")]

    processed_meetings = read_metadata()
    count_sent = 0
    for meeting in meetings_2025:
        meeting_key = meeting["meeting_key"]
        
        if meeting_key in processed_meetings:
            print(f"📂 Meeting {meeting_key} already processed, skipping...")
            continue

        # Construct S3 key correctly (you missed year and meeting name in your snippet)
        year = meeting.get("date_start", "")[:4]
        meeting_name = meeting.get("meeting_name", "").replace(" ", "_")
        s3_key = f"{S3_FOLDER}{year}_{meeting_name}_{meeting_key}.json"

        # Save meeting JSON to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(meeting)
        )
        print(f"✅ Stored meeting {meeting_key} in S3")

        # Send message to SQS
        message = {
            "meeting_key": meeting_key,
            "meeting_name": meeting.get("meeting_name")
        }
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(message)
        )
        print(f"📤 Sent meeting {meeting_key} to SQS")

        processed_meetings.append(meeting_key)
        count_sent += 1

    # Update metadata after processing all
    write_metadata(processed_meetings)

    return {
        "statusCode": 200,
        "body": f"✅ Finished. Sent {count_sent} new meetings to queue."
    }

