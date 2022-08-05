import os
import sys
import urllib.request
import pandas as pd
import argparse
from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name    
    url = params.url

    # Download and convert to csv
    print('starting download')
    csv_name = 'output.csv'
    if url[-7:] == 'parquet':
        df_pq = pd.read_parquet(url)
        print('download completed, converting to csv') 
        df_pq.to_csv(csv_name)
        print('csv conversion complete')
    elif url[-3:] == 'csv':
        urllib.request.urlretrieve(url, csv_name)
        print('csv download complete')
    
    

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    # Handles if csv's are added, is only used for shorter tables
    if url[-3:] == 'csv':
        pd.read_csv(csv_name)
        df.to_sql(name=table_name, con=engine, if_exists='replace')
        sys.exit(0)

    # Load in column headers first
    df = df.drop(df.columns[0], axis=1)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    looping = True
    # Loading in first 100k rows before looping through next
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while looping:
        try:
            df = next(df_iter)
        except StopIteration:
            looping = False
        else:
            # Removing blank first column left behind form parquet to csv conversion
            start_time = time()
            df = df.drop(df.columns[0], axis=1)
        
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')
            print('100k rows ingested in %.3f seconds' % (time() - start_time))

        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

# user, password, host, port, database name, table name, url to csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of the csv')


    args = parser.parse_args()
    main(args)

