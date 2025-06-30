import boto3
import json
import pandas as pd
import io
import os

s3 = boto3.client('s3')

S3_BUCKET = 'f1-75'
RAW_FOLDER_PREFIXES = {
    "meetings_raw": "raw_data/meetings_raw/",
    "sessions_raw": "raw_data/sessions_raw/",
    "drivers_raw": "raw_data/drivers_raw/",
    "laps_raw": "raw_data/laps_raw/"
}
TRANSFORMED_PREFIXES = {
    "meetings_raw": "transformed_data/meetings_transformed/",
    "sessions_raw": "transformed_data/sessions_transformed/",
    "drivers_raw": "transformed_data/drivers_transformed/",
    "laps_raw": "transformed_data/laps_transformed/"
}
METADATA_KEY = 'metadata/processed_transformed.json'

def load_metadata():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=METADATA_KEY)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            return {"processed": []}
        else:
            raise

def save_metadata(metadata):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=METADATA_KEY,
        Body=json.dumps(metadata)
    )

def list_all_json_keys(prefix):
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                keys.append(obj['Key'])
    return keys

def read_json_from_s3(key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = json.loads(obj['Body'].read().decode('utf-8'))
    if isinstance(data, dict):
        return [data]  # wrap single object in list
    elif isinstance(data, list):
        return data
    else:
        return []

def write_csv_to_s3(data, key):
    df = pd.DataFrame(data)
    if df.empty:
        print(f"⚠️ Skipping empty CSV for {key}")
        return
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    print(f"✅ Transformed and uploaded: {key}")

def lambda_handler(event, context):
    metadata = load_metadata()
    already_processed = set(metadata.get("processed", []))
    newly_processed = []

    for record in event['Records']:
        body = json.loads(record['body'])

        for section, do_process in body.items():
            if not do_process or section not in RAW_FOLDER_PREFIXES:
                continue

            raw_prefix = RAW_FOLDER_PREFIXES[section]
            transformed_prefix = TRANSFORMED_PREFIXES[section]

            json_keys = list_all_json_keys(raw_prefix)
            print(f"📦 Found {len(json_keys)} raw files for {section}")

            for raw_key in json_keys:
                if raw_key in already_processed:
                    print(f"🔁 Already transformed {raw_key}, skipping...")
                    continue

                try:
                    data = read_json_from_s3(raw_key)
                    transformed_key = raw_key.replace(raw_prefix, transformed_prefix).replace('.json', '.csv')
                    write_csv_to_s3(data, transformed_key)
                    newly_processed.append(raw_key)
                except Exception as e:
                    print(f"❌ Failed transforming {raw_key}: {str(e)}")

    metadata["processed"] = list(set(already_processed).union(newly_processed))
    save_metadata(metadata)

    return {
        "statusCode": 200,
        "body": f"✅ Transformed {len(newly_processed)} new files."
    }

