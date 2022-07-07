import argparse
from ast import While
import pandas as pd
import os
from sqlalchemy import create_engine
from time import time

def main(params):
    dtb_user = params.dtb_user
    dtb_password = params.dtb_password
    dtb_host = params.dtb_host
    dtb_port = params.dtb_port
    dtb_name = params.dtb_name
    dtb_table_name = params.dtb_table_name
    csv_url = params.csv_url

    csv_name = 'output.csv'
    os.system(f"wget {csv_url} -O {csv_name}")

    df = pd.read_csv('output.csv')

    # engine = create_engine('postgresql://root:tom_123@localhost:5432/ny_taxi')
    engine = create_engine(f'postgresql://{dtb_user}:{dtb_password}@{dtb_host}:{dtb_port}/{dtb_name}')
    # print(df.head(n=0))
    # print(pd.io.sql.get_schema(df, name = dtb_table_name))
    df.head(n=0).to_sql(name=dtb_table_name, con=engine, if_exists='replace')
    

    # # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    df_iter = pd.read_csv(csv_name, iterator = True , chunksize = 100000)

    df = next(df_iter)
    print(df = next(df_iter))
    # df.to_sql(name = 'yellow_tripdata_trip', con = engine, if_exists = 'append')

    try:
        while True:
            t_start = time()
            df = next(df_iter)
            # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name = dtb_table_name, con = engine, if_exists = 'append')
            t_end = time()
            print('inserting.., took %.3f' %(t_end - t_start))
    except:
        print("Finished") 
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import CSV data to postgres')
    parser.add_argument('--dtb_user',help='database username for postgres')
    parser.add_argument('--dtb_password',help='database password for postgres')
    parser.add_argument('--dtb_host',help='database host for postgres')
    parser.add_argument('--dtb_port',help='database port for postgres')
    parser.add_argument('--dtb_name',help='database name for postgres')
    parser.add_argument('--dtb_table_name',help='database table name for postgres')
    parser.add_argument('--csv_url',help='csv url for dowload')
    args = parser.parse_args()
    main(args)

#dtb_user
#dtb_password
#dtb_host
#dtb_port
#dtb_name
#dtb_table_name
#csv_path


    
