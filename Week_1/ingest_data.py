import pandas as pd
import pyarrow
from sqlalchemy import create_engine
import os
import argparse
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #os.system(f"wget {url} -O {parquet_name}")
    #df = pd.read_parquet(parquet_name)


    df = pd.read_parquet(url)
    
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    
    df.to_sql(name=table_name, con=engine, if_exists='append')
    print("success!")


if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)