import pandas as pd
import psycopg2
import glob

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
        
    cur.close()
    return colum_names, data

def insert(df, filename):
    
    fk = list(filter(lambda x: x.lower().startswith('id_') and x[3:].lower() != filename[:-4], df))
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
        for row in df.values:
            cur.execute(
                f"INSERT INTO {filename[:-4]} VALUES {(tuple(row))} ON CONFLICT DO NOTHING"
            )
        
    except Exception as e:
        print(f"\nErro ao inserir rows: {e}.")
    else:
        conn.commit()
        print("\nRows inseridas com sucesso.")

    cur.close()
    conn.close()


def execute():
    try:
        connect_db()
    except Exception as e:
        print(f"Conexão recusada. Erro: {e}")
    else:
        print("Conexão estabelecida.")

    for filename in glob.iglob(f'*.csv'):
        df = pd.read_csv(f"{filename}")
        filename = filename
        insert(df.dropna(axis=1), filename)