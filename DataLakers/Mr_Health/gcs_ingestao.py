from google.cloud import storage
import pandas as pd
from db_ingestao import connect_db
import os
import glob

# Source code responsável por realizar upload do Banco de Dados para o Google Cloud Storage

def upload_tbls(mode="std", tbls_name=str): # all: upload all tables; std: upload new table in folder
    
    std_path = []
    
    if mode == 'std':
        
        data = tbls_name
    
    if mode == 'all':
        # Collect table names from db
        _, data = connect_db("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

    # Instantiates a client
    storage_client = storage.Client()

    # The name for the bucket
    bucket_name = "mr_health"

    # The bucket on GCS in which to write the CSV file
    bucket = storage_client.get_bucket(bucket_name)

    # Iterate over tbl to persist in GCS
    for tbl in data:
        tbl_name = tbl[0] if mode == 'all' else tbl
        
        columns, tbl_data = connect_db(f"SELECT * FROM {tbl_name}") 
        df = pd.DataFrame(tbl_data, columns=columns)

        # Path to the local file to upload
        source_file_path = "./tables/"
        
        std_path += [source_file_path + f"ingestion_layer/{tbl_name}.csv"]

        df.to_csv(source_file_path + f"ingestion_layer/{tbl_name}.csv", index=False, sep=';')

        # Upload the file to GCS
    rel_paths = glob.glob(source_file_path + '/**', recursive=True) if mode == 'all' \
                                else std_path
    bucket = storage_client.get_bucket(bucket_name)
    for local_file in rel_paths:
        remote_path = f'{"/".join(local_file.split(os.sep)[1:])}' if mode == 'all' \
                    else f'{"/".join(local_file.split("/")[2:])}'
        if os.path.isfile(local_file):
            blob = bucket.blob(remote_path)
            blob.upload_from_filename(local_file)
