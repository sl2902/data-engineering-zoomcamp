import io
import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import time
from pathlib import Path
import argparse

"""
Pre-reqs: 
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
url = r"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "de-hw4")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(str(object_name) if isinstance(object_name, Path) else object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    """Copy file from remote location to GCS"""
    Path(f"data/{service}").mkdir(parents=True, exist_ok=True)
    for month in range(1, 13):
        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month:02}"
        out_path = Path(f"data/{service}/{file_name}.parquet")

        # download it using requests via a pandas df
        if not Path.is_file(out_path):
            url = f"{base_url}{service}"
            dataset_url = f"{url}/{file_name}.csv.gz"
            green_schema_dict = {
                    'VendorID': 'Int64',
                    'passenger_count': 'Int64',
                    'trip_distance': 'float64',
                    'RatecodeID': 'Int64',
                    'store_and_fwd_flag': 'object',
                    'PULocationID': 'Int64',
                    'DOLocationID': 'Int64',
                    'payment_type': 'Int64',
                    'fare_amount': 'float64',
                    'extra': 'float64',
                    'mta_tax': 'float64',
                    'tip_amount': 'float64',
                    'tolls_amount': 'float64',
                    'improvement_surcharge': 'float64',
                    'total_amount': 'float64',
                    'congestion_surcharge': 'float64',
                    'trip_type': 'Int64',
                    'lpep_pickup_datetime': 'string',
                    'lpep_dropoff_datetime': 'string',
                    'ehail_fee': 'float64'
                    }
            yellow_schema_dict = {
                    'VendorID': 'Int64',
                    'passenger_count': 'Int64',
                    'trip_distance': 'float64',
                    'RatecodeID': 'Int64',
                    'store_and_fwd_flag': 'object',
                    'PULocationID': 'Int64',
                    'DOLocationID': 'Int64',
                    'payment_type': 'Int64',
                    'fare_amount': 'float64',
                    'extra': 'float64',
                    'mta_tax': 'float64',
                    'tip_amount': 'float64',
                    'tolls_amount': 'float64',
                    'improvement_surcharge': 'float64',
                    'total_amount': 'float64',
                    'congestion_surcharge': 'float64',
                    'trip_type': 'Int64',
                    'tpep_pickup_datetime': 'string',
                    'tpep_dropoff_datetime': 'string',
                    'ehail_fee': 'float64'
                    }
            if service == "green":
                df = pd.read_csv(dataset_url, low_memory=False, dtype=green_schema_dict)
            elif service == "yellow":
                df = pd.read_csv(dataset_url, low_memory=False, dtype=yellow_schema_dict)
            elif service == "fhv":
                df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
                df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
                df["SR_Flag"] = df["SR_Flag"].astype("Int64")
                df["PUlocationID"] = df["PUlocationID"].astype("Int64")
                df["DOlocationID"] = df["DOlocationID"].astype("Int64")
        else:
            pass
            # performing this step as in Bigquery when
            # creating external table, vendorid is stored
            # as float and casting it to int throws error
            # the error is not resolved even after specifying
            # a schema.
            # Note - this fixed the issue with vendorid, but
            # caused an issue with the macro case statement.
            # I have to specify the dtype for each field as shown
            # above in the if-statement
            # dataset_url = out_path
            # df = pd.read_parquet(dataset_url)
            # for col in df.columns:
            #     df[col] = df[col].astype(str)
        # pa_table = pa.Table.from_pandas(df, schema=schema)
        # pq.write_table(pa_table, out_path)
        
        df.to_parquet(out_path, engine="pyarrow")

        

        print(f"Local: {out_path}")

        # read it back into a parquet file
        # df = pd.read_csv(file_name)
        # file_name = file_name.replace('.csv', '.parquet')
        # df.to_parquet(file_name, engine='pyarrow')
        # print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, out_path, out_path)
        print(f"GCS: {service}/{out_path}")

if __name__ == "__main__":
    year = 2020
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    color = "green"
    start = time.time()
    parser = argparse.ArgumentParser("ETL Web to GCS")
    parser.add_argument("--color", type=str, help="Input color of taxi. Choices are yellow/green", default="green")
    parser.add_argument("--year", type=int, help="Add year of data upload", default=2021)
    parser.add_argument("--months", type=int, nargs="+", help="List of months to process", default=[1])
    # parser.add_argument("--url", type=str, help="NYC saxi Source url")
    args = parser.parse_args()

    if args.year:
        year = args.year 
    if args.color:
        color = args.color
    if args.months:
        months = list(args.months)
    # if args.url:
    #     url = args.url

    web_to_gcs(year, color)
    end = time.time()
    print(f"Web to GCS file transmission completed {(end-start)/60} mins")