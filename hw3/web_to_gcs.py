import io
import os
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import time
from pathlib import Path

"""
Pre-reqs: 
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
url = r"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "de-hw3")


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
        dataset_url = f"{url}/{file_name}.csv.gz"
        df = pd.read_csv(dataset_url)
        # r = requests.get(request_url)
        # pd.DataFrame(io.StringIO(r.text)).to_csv(f"../data/{file_name}")
        # df = pd.DataFrame(io.StringIO(r.text))
        # df.columns = df.columns.astype(str)
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])
        df["PUlocationID"] = df["PUlocationID"].astype("Int64")
        df["DOlocationID"] = df["DOlocationID"].astype("Int64")
        df["SR_Flag"] = df["SR_Flag"].astype("Int64")
        # schema = pa.scalar([
        #     pa.field("dispatching_base_num", pa.string()),
        #     pa.field("pickup_datetime", pa.timestamp()),
        #     pa.field("dropOff_datetime", pa.timestamp()),
        #     pa.field("PUlocationID", pa.int64()),
        #     pa.field("DOlocationID", pa.int64()),
        #     pa.field("SR_Flag", pa.int64()),
        #     pa.field("Affiliated_base_number", pa.string())
        # ])
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
    start = time.time()
    web_to_gcs('2019', 'fhv')
    end = time.time()
    print(f"Web to GCS file transmission completed {(end-start)/60} mins")