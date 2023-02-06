from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_cloud_storage, bigquery_query
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from typing import List
from google.cloud import bigquery
from datetime import timedelta

@task(name="Fetch data", log_prints=True)
def fetch(url: str) -> pd.DataFrame:
     """Read data from web into pandas DataFrame"""
     return pd.read_csv(url)

@task(name="Clean data", log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Change the dtype for the date fields"""
    if "lpep_pickup_datetime" in df.columns:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    elif "tpep_pickup_datetime" in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(f"Number of rows {df.shape[0]}")
    return df

@task(
    name="Persist data locally", 
    log_prints=True, 
    cache_key_fn=task_input_hash, 
    cache_expiration=timedelta(days=1)
)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(name="Store in GCS", log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-prefect-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@flow(name="Sub-flow", log_prints=True)
def etl_web_to_gcs(
    color: str,
    year: int,
    month: int
):
    """Sub-flow ETL flow to load data into GCS"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow(name="Parent flow", log_prints=True)
def etl_parent_flow(
    color: str,
    year: int,
    months: List[int]
):
    """Parent flow"""
    gcp_credentials = GcpCredentials.load("de-prefect-gcp-creds")
    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == "__main__":
    year = 2020
    months = [11]
    color = "green"
    etl_parent_flow(
        color,
        year,
        months
    )