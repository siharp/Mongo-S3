# libraries needed
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os
import json
import boto3
from botocore.client import Config
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

#function to extract data
def extract_data(config, mongo_uri, database_name, start_date, end_date, airline):

    # get filtering apptimestamp startdate - 30 days
    dt = datetime.utcfromtimestamp(start_date / 1000)
    new_dt = dt - timedelta(days=31)
    new_timestamp_ms = int(new_dt.timestamp() * 1000)

    collection_name = config['schema_name']

    client = None
    try:
        # Connect to the database
        client = MongoClient(mongo_uri)
        database = client[database_name]
        print(f"Connect to database {database_name}")
        query = {"db_created_at": {"$gte": start_date, "$lte": end_date}, "trip_airline_id": airline, "app_event_utc_timestamp": {"$gte": new_timestamp_ms }}
        collection = database[collection_name]
        cursor = collection.find(query).batch_size(10000)
        print(f"Quering data from {collection_name} where date betweend {globals()['start_date']} and {globals()['end_date']}")
        result = [document for document in cursor]
        print(f"Total number of query result data before transformation {len(result)}")
        return result

    except Exception as e:
        print(f'Error extracting data: {e}')
        return []

    finally:
        if client:
            client.close()

# function to transform data
def transform_data(data, config):
    if not data:
        print("No data to transform")
        return pd.DataFrame()

    col_clear = config['filter_col_dup']
    transform_temp = config['transformations']

    df = pd.DataFrame(data)
    df.drop_duplicates(subset=col_clear, inplace=True)

    for col, transformation in transform_temp.items():
        if col in df.columns:
            df.rename(columns={col: transformation["rename_to"]}, inplace=True)

            # Convert data type
            convert_to = transformation["convert_to"]
            new_col = transformation["rename_to"]

            if convert_to == "string":
                df[new_col] = df[new_col].astype(str)
            elif convert_to == "integer":
                df[new_col] = pd.to_numeric(df[new_col], errors="coerce")
            elif convert_to == "boolean":
                df[new_col] = df[new_col].astype(bool)
            elif convert_to == "date_without_timezone":
                df[new_col] = pd.to_datetime(df[new_col], unit="ms")

    df['run_date'] = datetime.now().strftime('%Y-%m-%d')
    print(f'Complete tranforming {file_name}')

    return df

def upload_to_s3(local_file, bucket_name, access_key, secret_key, s3_url, s3_region, run_date, airline, file_name):
    try:
        # Inisialisasi klien S3
        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
            region_name=s3_region,
        )

        # Path tujuan di S3
        s3_path = f"{run_date}/{airline}/{file_name}.parquet"

        # Upload file
        s3_client.upload_file(local_file, bucket_name, s3_path)
        print(f"Uploaded {local_file} to s3://{bucket_name}/{s3_path} \n")
    except Exception as e:
        print(f"Error uploading to S3: {e}")



# function to save data in local
def save_file(df, file_name, airline, bucket_name, access_key, secret_key, s3_url, s3_region):
    if not df.empty:
        # Path lokal
        local_dir = f"result/{run_date}/{airline}"
        os.makedirs(local_dir, exist_ok=True)
        local_path = f"{local_dir}/{file_name}.parquet"

        # Simpan file lokal
        df.to_parquet(local_path)
        count_data = df.shape[0]
        print(f"Successfully saved {count_data} rows to {local_path}")

        # Upload ke S3
        upload_to_s3(local_path, bucket_name, access_key, secret_key, s3_url, s3_region, run_date, airline, file_name)
    else:
        print('No data to save')


if __name__ == '__main__':

    mongo_uri = os.getenv('MONGO_URL', '')
    database_name = os.getenv('MONGO_DATABASE', '')
    start_date = os.getenv('START_DATE', '')
    end_date = os.getenv('END_DATE', '')
    airline = os.getenv('AIRLINE_ID', '')
    bucket_name = os.getenv('BUCKET_NAME', '')
    acces_key = os.getenv('ACCES_KEY', '')
    secret_key = os.getenv('SECRET_KEY', '')
    minio_url = os.getenv('MINIO_URL', '')
    minio_region = os.getenv('REGION', '')
    run_date= os.getenv('RUN_DATE', '')

    #convert date to millisecond format
    start_date_mil = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_date_mil = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp() * 1000)

    #load file config
    with open("config/configuration.json", "r") as file:
        config = json.load(file)
        template = config['collections_meta']

        for i in template:
            file_name = i['schema_name']

            # Extract, Transform, and Save
            data = extract_data(i, mongo_uri, database_name, start_date_mil, end_date_mil, airline)
            transformed_data = transform_data(data, i)
            save_file(transformed_data, file_name, airline, bucket_name, acces_key, secret_key, minio_url, minio_region)
