from google.cloud import storage
import pandas as pd
from db_ingestao import connect_db
import os
import glob

def upload_tbls():
    # Collect table names from db
    _, data = connect_db("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

    # Instantiates a client
    storage_client = storage.Client()

    # The name for the bucket
    bucket_name = "mr_health"

    # The bucket on GCS in which to write the CSV file
    bucket = storage_client.get_bucket(bucket_name)

    # Iterate over tbl to generate parquet and persist in GCS and BigQuery
    for tbl in data:
        tbl_name = tbl[0]
        
        columns, tbl_data = connect_db(f"SELECT * FROM {tbl_name}") 
        df = pd.DataFrame(tbl_data, columns=columns)

        #TODO Automate folder creation to parquet files
        # Partition table
        df = df.to_parquet(f'./tables/{tbl_name}.parquet', partition_cols="data_pedido") \
                    if tbl_name == 'pedido' else df.to_parquet(f'./tables/{tbl_name}.parquet')

        # Path to the local Parquet file you want to upload
        source_file_path = "/tables/"
        
        # Upload the Parquet file to GCS
        rel_paths = glob.glob("./" + source_file_path + '/**', recursive=True)
        bucket = storage_client.get_bucket(bucket_name)
        for local_file in rel_paths:
            remote_path = f'ingestion_layer/{"/".join(local_file.split(os.sep)[1:])}'
            if os.path.isfile(local_file):
                blob = bucket.blob(remote_path)
                blob.upload_from_filename(local_file)
