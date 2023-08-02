import pandas as pd
import psycopg2
import glob
import csv
import re

def connect_db(select=''):
    global cur, conn
    
    conn = psycopg2.connect(host="localhost", dbname="mr_health",
                        user="postgres", password="555495") 
    cur = conn.cursor()
    if select != '':
        cur.execute(f"{select}") 

        data = cur.fetchall()
        colum_names = [desc[0] for desc in cur.description]
    else:
        data = []
        colum_names = []
        
    return colum_names, data

def insert(df, filename):
    tbl_name = filename
    fk = list(filter(lambda x: x.lower().startswith('id_') and x[3:].lower() != tbl_name, df))
    if fk:
        try:
            for key in fk:
                for row in df[key]:
                    cur.execute(
                        f"""INSERT INTO {key[3:].lower()} ({key}) SELECT {row} WHERE NOT EXISTS 
                        (SELECT 1 FROM {key[3:].lower()} WHERE {key} = {row})"""
                    )
        except Exception as e:
            print(f"\nErro ao inserir FK: {e}")
        else:
            conn.commit()
            print("\nFK inserida com sucesso.")    
        
    try:
        
        id_tbl_name = f"id_{tbl_name}"
        cur.execute(f"SELECT {id_tbl_name} from {tbl_name};") # to verify if contains id in table
        tbl_name_id = cur.fetchall()
        tbl_name_id = [y[0] for y in tbl_name_id]
        df_update = df.drop(id_tbl_name.title(), axis=1)
    
        df_values = df.to_records(index=False)
        columns = tuple([value for value in df.describe()])
        columns_update = tuple([value for value in df_update.describe()])
        update_columns = tuple(["EXCLUDED." + value for value in columns_update])
        for row in df_values:
            cur.execute(f"""
                  INSERT INTO {tbl_name} ({", ".join(columns)})
                  VALUES ({", ".join(row)})
                  ON CONFLICT ({id_tbl_name})
                  DO UPDATE SET ({", ".join(columns_update)}) = ({(", ").join(update_columns)})
            """)

    except Exception as e:
        print(f"\nErro ao inserir rows: {e}.")
    else:
        conn.commit()
        print("\nRows inseridas com sucesso.")    


def execute():
    try:
        connect_db()
    except Exception as e:
        print(f"Conexão recusada. Erro: {e}")
    else:
        print("Conexão estabelecida.")
    try:
        for filename in glob.iglob('.\csv-tables\*.csv'):
            print(filename)
            with open(filename, 'r', encoding='utf-8') as csvfile:
                csv_reader = csv.DictReader(csvfile, delimiter=';', )
                data = list(csv_reader)
                for values in data:
                    for col, val in values.items():
                        if re.search(r'[a-zA-Z]', val):
                            values[col] = f"'{val}'"
                        if re.search(r'\d{2}/\d{2}/\d{4}', val):
                            values[col] = f"'{val}'"
            filename = filename.split("\\")[-1][:-4]
            df = pd.DataFrame(data)
            df.replace("", float("NaN"), inplace=True)
            df.dropna(axis='columns', how='all', inplace=True)
            insert(df, filename)
    except Exception as e:
        print(f"Erro: {e}")
    else:        
        cur.close()
        conn.close()