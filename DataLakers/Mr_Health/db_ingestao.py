import pandas as pd
import psycopg2
import glob
import csv
import re
import os

# Source code para inserir dados vindos do(s) CSV(s) no Banco de Dados

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

def insert(df, tbl_name):
    fk = list(filter(lambda x: x.lower().startswith('id_') and x[3:].lower() != tbl_name, df))
    try:
        contain_fk = connect_db(f"SELECT {', '.join(fk)} FROM {tbl_name}")
    except Exception as e:
        print(f"\nConexão recusada. Erro: {e}")
    else:
        print("\nConexão estabelecida.")

    if not contain_fk[1]:
        try:
            for key in fk:
                for row in df[key]:
                    cur.execute(
                        f"""INSERT INTO {key[3:].lower()} ({key}) SELECT {row} WHERE NOT EXISTS 
                        (SELECT 1 FROM {key[3:].lower()} WHERE {key} = {row})"""
                    )
        except Exception as e:
            print(f"\nErro ao inserir FK em {tbl_name}: {e}")
        else:
            conn.commit()
            print(f"\nFK inserida com sucesso em {tbl_name}.")
        
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
            if tbl_name == 'pedido':
                cur.execute(
                    f"""
                    INSERT INTO {tbl_name} ({", ".join(columns)})
                    VALUES ({", ".join(row)}) ON CONFLICT DO NOTHING
                    """
                )
            else:
                cur.execute(
                    f"""
                    INSERT INTO {tbl_name} ({", ".join(columns)})
                    VALUES ({", ".join(row)})
                    ON CONFLICT ({id_tbl_name})
                    DO UPDATE SET ({", ".join(columns_update)}) = ({(", ").join(update_columns)})
                    """
                )

    except Exception as e:
        print(f"\nErro ao inserir rows em {tbl_name}: {e}.")
    else:
        conn.commit()
        print(f"\nRows inseridas com sucesso em {tbl_name}.")

def execute():
    try:
        tbls_name = []
        for filename in glob.iglob('.\csv-tables\*.csv'):
            with open(filename, 'r', encoding='utf-8') as csvfile:
                csv_reader = csv.DictReader(csvfile, delimiter=';')
                data = list(csv_reader)
                for values in data:
                    for col, val in values.items():
                        if re.search(r'[a-zA-Z]', val):
                            values[col] = f"'{val}'"
                        if re.search(r'\d{4}/\d{2}/\d{2}', val):
                            values[col] = f"'{val}'"
            tbl_name = filename.split("\\")[-1][:-4]
            df = pd.DataFrame(data)
            df.replace("", float("NaN"), inplace=True)
            df.dropna(axis='columns', how='all', inplace=True)
            insert(df, tbl_name)
            tbls_name += [tbl_name]

    except Exception as e:
        print(f"Erro: {e}")

    else:        
        cur.close()
        conn.close()
        return tbls_name

def empty_csv_folder():
        files = glob.glob('.\csv-tables\*.csv')
        for file in files:
            os.remove(file)