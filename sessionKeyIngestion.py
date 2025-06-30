import boto3
import json
import urllib3

http = urllib3.PoolManager()
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

SESSION_QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/253613561634/Session_id_Q'
S3_BUCKET = 'f1-75'
S3_FOLDER = 'raw_data/sessions_raw/'
METADATA_KEY = 'metadata/processed_sessions.json'

def read_metadata():
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=METADATA_KEY)
        data = response['Body'].read()
        processed_sessions = json.loads(data)
        print(f"📋 Loaded metadata with {len(processed_sessions)} sessions")
        return processed_sessions
    except s3.exceptions.NoSuchKey:
        print("⚠️ Metadata file not found, initializing empty list")
        return []
    except Exception as e:
        print(f"❌ Error reading metadata: {e}")
        return []

def write_metadata(processed_sessions):
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=METADATA_KEY,
            Body=json.dumps(processed_sessions)
        )
        print(f"✅ Updated metadata with {len(processed_sessions)} sessions")
    except Exception as e:
        print(f"❌ Error writing metadata: {e}")

def lambda_handler(event, context):
    processed_sessions = read_metadata()
    count_sent = 0

    for record in event['Records']:
        body = json.loads(record['body'])
        meeting_key = body.get('meeting_key')

        if not meeting_key:
            print("❌ No meeting_key found in message.")
            continue

        print(f"🔍 Fetching sessions for meeting_key: {meeting_key}")
        url = f"https://api.openf1.org/v1/sessions?meeting_key={meeting_key}"
        response = http.request('GET', url)

        if response.status != 200:
            print(f"❌ Failed to fetch sessions for meeting_key: {meeting_key}")
            continue

        sessions = json.loads(response.data.decode('utf-8'))

        for session in sessions:
            session_key = session.get('session_key')
            session_name = session.get('session_name')
            if not session_key:
                print("⚠️ Skipping session with missing session_key.")
                continue

            if session_key in processed_sessions:
                print(f"📂 Session {session_key} already processed, skipping...")
                continue

            s3_key = f"{S3_FOLDER}{meeting_key}/{session_key}.json"
            try:
                s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
                print(f"📂 Session {session_key} already in S3, skipping upload...")
                continue
            except s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] != "404":
                    raise

            # Upload session JSON to S3
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(session)
            )
            print(f"✅ Stored session {session_key} in S3")

            # Send message to SQS
            msg = {
                "meeting_key": meeting_key,
                "session_key": session_key,
                "session_name": session_name
            }
            sqs.send_message(
                QueueUrl=SESSION_QUEUE_URL,
                MessageBody=json.dumps(msg)
            )
            print(f"📤 Sent session_key {session_key} to Session_id_Q")

            processed_sessions.append(session_key)
            count_sent += 1

    # Update metadata after processing all new sessions
    write_metadata(processed_sessions)

    return {
        "statusCode": 200,
        "body": f"✅ Processed sessions for all meeting_keys. Sent {count_sent} new sessions."
    }

