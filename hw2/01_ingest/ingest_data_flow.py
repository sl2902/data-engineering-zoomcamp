import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector
from pathlib import Path
import numpy as np

@task(log_prints=True, tags=["download"], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_data(url: str, output_dir="data"):
    """Download data from NYC Taxi trips website"""
    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"
    
    os.system(f"wget {url} -O ../{output_dir}/{csv_name}")
    return csv_name

@task(log_prints=True)
def prepare_data(filename: str):
    """Read and prepare the Taxi trips dataset"""
    chunksize=100000
    df = pd.read_csv(Path(filename).resolve())
    indices = np.arange(chunksize, df.shape[0], chunksize)
    for split in np.array_split(df, indices):
        yield split

@task(log_prints=True)
def transform_data(df: pd.DataFrame, chunk: int):
    """Do some cleanup. Drop rows with 0 passenger counts"""
    print(f"pre: missing passenger count in chunk {chunk}: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count in chunk {chunk}: {df['passenger_count'].isin([0]).sum()}")

@task(log_prints=True, retries=0)
def load_data(engine, table_name: str, df: pd.DataFrame, chunk: int):
    """Load data into Postgres"""
    if chunk == 0:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print("Loading data...")
    df.to_sql(name=table_name, con=engine, if_exists="append")

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")

@flow(name="Ingest Data")
def main_flow(table_name: str = "yellow_taxi_trips", output_dir="data"):
    chunk = 0
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    filename = download_data(csv_url)
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        for df in prepare_data(f"../{output_dir}/{filename}"):
            transform_data(df, chunk)
            load_data(engine, table_name, df, chunk)
            chunk += 1

if __name__ == "__main__":
    main_flow(table_name = "yellow_trips")