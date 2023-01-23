#!/usr/bin/env python
# coding: utf-8
import os
from sqlalchemy import create_engine
import pandas as pd
import argparse

#url = "<https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv>"

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    
    file_name='taxi+_zone_lookup.csv'

    df_zones = pd.read_csv(file_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')
    print(f'data ingestion completed..')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')

    args = parser.parse_args()

    main(args)