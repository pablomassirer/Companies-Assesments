from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from db_ingestao import execute, empty_csv_folder
from gcs_ingestao import upload_tbls

# Primeiro arquivo a ser executado.
# Source code responsável por monitorar alteração na pasta 'csv-tables' e realizar a partir dai
# inserção no banco de dados com o método importado 'execute()' e se bem sucedido 
# limpar a pasta citada anteriromente com método 'empty_csv_folder()' 
# e realizar o upload da(s) tabela(s) para o Google Cloud Storage com métod 'upload_tbls()'

class FileCreateHandler(FileSystemEventHandler):
    def on_created(self, event):
        try:
            tbls_name = execute()
        except Exception as e:
            print(f"Erro: {e}. Arquivo não criado.")
        else:
            print("Arquivo criado")
            try:
                empty_csv_folder()
                # mode -> all: upload all tables; std: upload new csv table in folder (can be ommited)
                upload_tbls(mode='all', tbls_name=tbls_name) 
            except Exception as e:
                print(f"Erro: {e}. Upload não realizado.")
            else:
                print('Upload concluido.')  
                  

if __name__ == "__main__":

    event_handler = FileCreateHandler()

    # Create an observer.
    observer = Observer()

    # TODO: Create relative path
    # Attach the observer to the event handler.
    observer.schedule(event_handler, "./csv-tables", recursive=True)

    # Start the observer.
    observer.start()

    try:
        while observer.is_alive():
            observer.join(1)
    finally:
        observer.stop()
        observer.join()
