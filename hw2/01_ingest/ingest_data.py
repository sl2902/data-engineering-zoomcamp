import pandas as pd
from sqlalchemy import create_engine
import psycopg2
# import pyodbc
import time
import os

def ingest_data(
    user: str, 
    password: str, 
    host: str, 
    port: str, 
    db:str, 
    table_name: str, 
    url:str
):
    """Ingest data into postgres db"""
    if url.endswith(".csv.gz"):
        csv_name = "yellow_tripdata_2021-01.csv.gz"
    else:
        csv_name = "output.csv"
    
    os.system(f"wget {url} -O {csv_name}")
    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)

    df_iter = pd.read_csv(
        csv_name, 
        iterator=True, 
        chunksize=100_000,
        low_memory=False
    )
    df = next(df_iter)
    # change the dtype
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    # add header
    df.head(n=0).to_sql(
        name=table_name, 
        con=engine, 
        if_exists='replace'
    )
    # append the first 99_999 rows
    print("Loading data...")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    for batch in df_iter:
        t_start = time.time()
        batch.tpep_pickup_datetime = pd.to_datetime(batch.tpep_pickup_datetime)
        batch.tpep_dropoff_datetime = pd.to_datetime(batch.tpep_dropoff_datetime)
        batch.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time.time()
        print('Inserted another chunk, took %.3f second' % (t_end - t_start))
    
if __name__ == '__main__':
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    ingest_data(user, password, host, port, db, table_name, csv_url)