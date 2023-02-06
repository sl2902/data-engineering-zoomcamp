from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
import time
from datetime import timedelta
from typing import List
import argparse


@task(log_prints=True, tags=["download"])
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    print(f"Number of rows in {dataset_url.split('/')[-1]} {df.shape[0]}")
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == "green":
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    elif color == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(f"Number of rows {df.shape[0]}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-prefect-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@flow(log_prints=True)
def etl_web_to_gcs(
    dataset_url: str,
    color: str,
    year: int,
    month: int,
    dataset_file: str
) -> None:
    """The main ETL function"""
    # color = "green"
    # year = 2020
    # month = 1
    # dataset_file = f"{color}_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    # df_clean = clean(df)
    path = write_local(df, color, dataset_file)
    write_gcs(path)


# if __name__ == "__main__":
#     etl_web_to_gcs()

@flow(log_prints=True)
def etl_parent_flow(
    color: str,
    year: int,
    months: List[int]
):
    """Parent flow"""
    for month in months:
        dataset_file =  f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        etl_web_to_gcs(
            dataset_url,
            color,
            year,
            month,
            dataset_file
        )

if __name__ == "__main__":
    year = 2020
    months = [1]
    color = "green"

    parser = argparse.ArgumentParser("ETL Web to GCS")
    parser.add_argument("--color", type=str, help="Input color of taxi. Choices are yellow/green", default="green")
    parser.add_argument("--year", type=int, help="Add year of data upload", default=2021)
    parser.add_argument("--months", type=int, nargs="+", help="List of months to process", default=[1])
    args = parser.parse_args()

    if args.year:
        year = year 
    if args.color:
        color = color
    if args.months:
        months = list(months)

    etl_parent_flow(
        color,
        year,
        months
    )