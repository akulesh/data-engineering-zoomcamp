import argparse
import os
import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

from constants import BLOCK_NAME, PROJECT_ID, CREDS_BLOCK_NAME


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int):
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load(BLOCK_NAME)

    local_path = "data/gcs/"
    path = os.path.join(local_path, gcs_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)

    gcs_block.get_directory(from_path=gcs_path, local_path=path)
    return os.path.join(local_path, gcs_path)


@task()
def transform(path) -> pd.DataFrame:
    """Data loading example"""
    return pd.read_parquet(path)


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load(CREDS_BLOCK_NAME)

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id=PROJECT_ID,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(color="yellow", year=2021, months: list = None):
    """Main ETL flow to load data into Big Query"""
    logger = get_run_logger()

    if months is None:
        months = [1, 2]

    n_rows = 0
    for month in months:
        path = extract_from_gcs(color, year, int(month))
        df = transform(path)
        n_rows += df.shape[0]
        logger.info(f"Total number of rows: {n_rows}")
        write_bq(df)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--color", default="yellow")
    parser.add_argument("--year", type=int, default=2022)
    parser.add_argument("--months", type=str)

    args = parser.parse_args()
    color = args.color
    year = args.year
    months = args.months.split(",")

    etl_gcs_to_bq(color=color, year=year, months=months)
