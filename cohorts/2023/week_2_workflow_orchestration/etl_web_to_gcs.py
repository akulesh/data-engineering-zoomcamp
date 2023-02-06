import argparse
import os

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from constants import BLOCK_NAME


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    return pd.read_csv(dataset_url)


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(df.head(2))

    cols = [
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "dpep_dropoff_datetime",
        "dpep_pickup_datetime",
    ]

    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    print(f"rows: {len(df)}")
    print(f"columns: {df.dtypes}")

    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str):
    """Write DataFrame out locally as parquet file"""

    output_dir = f"data/{color}"
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load(BLOCK_NAME)
    gcs_block.upload_from_path(from_path=path, to_path=path)

    print(f"Destination on GCS: {path}")

    return


@flow()
def etl_web_to_gcs(color="yellow", year=2021, month=1) -> None:
    """The main ETL function"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    print(f"URL: {dataset_url}")

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    print(f"Data saved on the local disk: {path}")
    write_gcs(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--color", default="yellow")
    parser.add_argument("--year", type=int, default=2022)
    parser.add_argument("--month", type=int, default=1)

    args = parser.parse_args()
    color = args.color
    year = args.year
    month = args.month

    etl_web_to_gcs(color=color, year=year, month=month)
