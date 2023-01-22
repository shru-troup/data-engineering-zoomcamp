import pandas as pd
import argparse
from time import time
import os

from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    filename = 'output.parquet'
    os.system(f"wget {url} -O {filename}")
    df = pd.read_parquet(filename)
    print(f'Size of the file: {df.shape}')
    print(df.head())
    print('Creating Postgres engine via SQLAlchemy...')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(pd.io.sql.get_schema(df, name=f'{table_name}', con=engine))
    t_start = time()
    df = df.head(100000)
    df.to_sql(name=table_name, con=engine, if_exists='replace')
    t_end = time()
    print(f'Time to ingest the data: {(t_end - t_start)}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest PARQUET data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)