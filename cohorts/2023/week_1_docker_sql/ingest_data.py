#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    trips_url = params.trips_url
    zones_url = params.zones_url

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    csv_name = 'output.csv.gz' if trips_url.endswith('.csv.gz') else 'output.csv'
    os.system(f"wget {trips_url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    drop_if_exists = True
    while True:
        try:
            t_start = time()

            df = next(df_iter)

            if drop_if_exists:
                df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
                drop_if_exists = False

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name="yellow_taxi_data", con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

    csv_name = 'output.csv.gz' if zones_url.endswith('.csv.gz') else 'output.csv'
    os.system(f"wget {zones_url} -O {csv_name}")
    df_zones = pd.read_csv(csv_name)
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--trips_url', required=True, help='url of the csv file')
    parser.add_argument('--zones_url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
