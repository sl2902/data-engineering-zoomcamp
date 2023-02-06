from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_cloud_storage, bigquery_query
from prefect_gcp import GcpCredentials
from typing import List
from google.cloud import bigquery


@task(log_prints=True, tags=["download"], retries=0)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    file_suffix = f"{color}_tripdata_{year}-{month:02}.parquet"
    local_dir = f"data/{color}/"
    gcs_path = f"{local_dir}{file_suffix}"
    gcs_block = GcsBucket.load("de-prefect-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"./{gcs_path}")


@task()
def read_file(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    print(f"Number of rows {df.shape[0]}")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-prefect-gcp-creds")

    df.to_gbq(
        destination_table="de_bq_hw2.ny_taxi_rides",
        project_id="data-eng-375913",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(
    uri: str,
    color: str,
    year: int,
    month: int
):
    """Main ETL flow to load data into Big Query from GCS"""
    # path = extract_from_gcs(color, year, month)
    # df = transform(path)
    # client = bigquery.Client()
    # # if you want to prepend the project id to the dataset id
    # dataset_id = f"{client.project}.de_bq_hw2"
    # dataset = bigquery.Dataset(dataset_id)
    # dataset.location = "asia-south1"
    # try:
    #     dataset = client.create_dataset(dataset, timeout=30)
    # except Exception as e:
    #     raise Exception("Failed to create BigQuery Dataset")

    job_config = bigquery.LoadJobConfig()
    # google.api_core.exceptions.BadRequest: 400 Error while reading data, error message: 
    # Error detected while parsing row starting at position: 0. Error: Bad character (ASCII 0) encountered. 
    # File: gs://de-hw2/data/yellow/yellow_tripdata_2019-02.parquet
    # https://stackoverflow.com/questions/73782466/how-to-preserve-ascii-control-characters-when-loading-into-bigquery
    job_config._properties["load"]["preserve_ascii_control_characters"] = True
    job_config._properties["autodetect"]=True
    print(job_config._properties)

    if color == "green":
        schema = [
            bigquery.SchemaField('VendorID', 'STRING', mode="REQUIRED"),
            bigquery.SchemaField('lpep_pickup_datetime', 'TIMESTAMP', mode="REQUIRED"),
            bigquery.SchemaField('lpep_dropoff_datetime', 'TIMESTAMP', mode="REQUIRED"),
            bigquery.SchemaField('store_and_fwd_flag', 'STRING', mode="REQUIRED"),
            bigquery.SchemaField('RatecodeID', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('PULocationID', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('DOLocationID', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('passenger_count', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('trip_distance', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('fare_amount', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('extra', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('mta_tax', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('tip_amount', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('tolls_amount', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('ehail_fee', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('improvement_surcharge', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('total_amount', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('payment_type', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('trip_type', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('congestion_surcharge', 'FLOAT64', mode="REQUIRED"),
    ]
    elif color == "yellow":
        schema = [
            bigquery.SchemaField('VendorID', 'STRING', mode="REQUIRED"),
            bigquery.SchemaField('tpep_pickup_datetime', 'TIMESTAMP', mode="REQUIRED"),
            bigquery.SchemaField('tpep_dropoff_datetime', 'TIMESTAMP', mode="REQUIRED"),
            bigquery.SchemaField('passenger_count', 'INT64', mode="REQUIRED"),
            bigquery.SchemaField('trip_distance', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('RatecodeID', 'INT64', mode="REQUIRED"),
            bigquery.SchemaField('store_and_fwd_flag', 'STRING', mode="REQUIRED"),
            bigquery.SchemaField('PULocationID', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('DOLocationID', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('payment_type', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('fare_amount', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('extra', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('mta_tax', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('tip_amount', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('tolls_amount', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('improvement_surcharge', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('total_amount', 'FLOAT64', mode="REQUIRED"),
            bigquery.SchemaField('congestion_surcharge', 'FLOAT64', mode="REQUIRED"),
        ]

    gcp_credentials = GcpCredentials.load("de-prefect-gcp-creds")
    result = bigquery_load_cloud_storage(
        dataset="de_bq_hw2",
        table="ny_taxi_rides",
        uri=uri,
        gcp_credentials=gcp_credentials,
        location="asia-south1",
        job_config=job_config,
        schema=schema
    )
    return result

@flow(log_prints=True)
def etl_subflow(
    color: str,
    year: int,
    month: int
):
    path = extract_from_gcs(color, year, month)
    df = read_file(path)
    write_bq(df)

@flow(log_prints=True)
def etl_parent_flow(
    base_uri: str,
    color: str,
    year: int,
    months: List[int]
):
    """Parent flow"""
    gcp_credentials = GcpCredentials.load("de-prefect-gcp-creds")
    for month in months:
        etl_subflow(color, year, month)
    query = '''
        SELECT COUNT(*)
        FROM `de_bq_hw2.ny_taxi_rides`
    '''
    result = bigquery_query(
        query, 
        gcp_credentials=gcp_credentials,
        location='asia-south1'
    )
    print(f"Number of rows imported {result[0][0]}")
        # uri = base_uri + f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"

        # result = etl_gcs_to_bq(
        #     uri,
        #     color,
        #     year,
        #     month
        # )
        # print(f"Successfully loaded data into BQ {result}")

if __name__ == "__main__":
    base_uri = f"gs://de-hw2/"
    year = 2019
    months = [2, 3]
    color = "yellow"
    etl_parent_flow(
        base_uri,
        color,
        year,
        months
    )