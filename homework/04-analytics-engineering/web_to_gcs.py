import io
import os
import requests
import datetime
from dateutil.relativedelta import *
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
from pathlib import Path
import polars as pl

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage python-dotenv`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# Load environment variables

load_dotenv()

print("Launching import service...")
prefix_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET")
SERVICE_ACCOUNT_JSON = os.environ.get("SERVICE_ACCOUNT_JSON")
print(f"Bucket is: {BUCKET}")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    print(f"Uploading {local_file} to GCS")
    blob.upload_from_filename(local_file)


def web_to_gcs(starting_date, ending_date, service):
    print(f"Starting Service : {service}")
    start = datetime.datetime.strptime(starting_date, '%Y%m')
    end = datetime.datetime.strptime(ending_date, '%Y%m')
    step = relativedelta(months=+1)

    while start <= end: 
        # sets the month part of the file_name string
        month_file=start.date().strftime("%Y-%m")
        print(f"Starting for {service}_tripdata_{month_file}.csv.gz ")

        # csv file_name
        file_name = f"{service}_tripdata_{month_file}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{prefix_url}{service}/{file_name}"
        if (not os.path.isfile(file_name)):
            r = requests.get(request_url)
            open(file_name, 'wb').write(r.content)

        print(f"Local: {file_name}")

        # define type
        if service == "green":
            print("Using lpep_pickup_datetime as parse_dates.")
            taxi_dtypes = {
                        'VendorID': pl.Int64(),
                        'passenger_count': pl.Int64(),
                        'trip_distance': float,
                        'RatecodeID':pl.Int64(),
                        'store_and_fwd_flag':str,
                        'PULocationID':pl.Int64(),
                        'DOLocationID':pl.Int64(),
                        'payment_type': pl.Int64(),
                        'fare_amount': float,
                        'extra':float,
                        'mta_tax':float,
                        'tip_amount':float,
                        'tolls_amount':float,
                        'improvement_surcharge':float,
                        'total_amount':float,
                        'congestion_surcharge':float,
                        'trip_type': pl.Int64(),
                        'lpep_pickup_datetime': pl.Datetime(),
                        'lpep_dropoff_datetime': pl.Datetime()
                    }
        if service == "yellow":
            print("Using tpep_pickup_datetime as parse_dates.")
            taxi_dtypes = {
                        'VendorID': pl.Int64(),
                        'passenger_count': pl.Int64(),
                        'trip_distance': float,
                        'RatecodeID':pl.Int64(),
                        'store_and_fwd_flag':str,
                        'PULocationID':pl.Int64(),
                        'DOLocationID':pl.Int64(),
                        'payment_type': pl.Int64(),
                        'fare_amount': float,
                        'extra':float,
                        'mta_tax':float,
                        'tip_amount':float,
                        'tolls_amount':float,
                        'improvement_surcharge':float,
                        'total_amount':float,
                        'congestion_surcharge':float,
                        'tpep_pickup_datetime': pl.Datetime(),
                        'tpep_dropoff_datetime': pl.Datetime()
                    }
        if service == "fhv":
            print("Using pickup_datetime as parse_dates.")
            taxi_dtypes = {
                        'dispatching_base_num': str,
                        'PUlocationID': pl.Int64(),
                        'DOlocationID':pl.Int64(),
                        'SR_Flag': pl.Int64(),
                        'Affiliated_base_number': str,
                        'pickup_datetime': pl.Datetime(),
                        'dropOff_datetime': pl.Datetime(),
                    }

        # read it back into a parquet file
        df = pl.read_csv(file_name, dtypes=taxi_dtypes)
        os.remove(file_name)
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.write_parquet(file_name)
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}__tripdata/{file_name}", file_name)
        os.remove(file_name)
        print(f"GCS: {service}_tripdata/{file_name}")

        # add 1 month
        start += step

services = ['green','yellow']
# services = ['fhv']
starting_date = "201901"
ending_date = "202012"

print("Starting upload ...")
for service in services:
    web_to_gcs(starting_date, ending_date, service)