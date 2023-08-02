from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from db_ingestao import execute
from gcs_ingestao import upload_tbls

class FileCreateHandler(FileSystemEventHandler):
    def on_created(self, event):
        try:
            execute()
        except Exception as e:
            print(f"Erro: {e}. Arquivo não criado.")
        else:
            print("Arquivo criado")
            try:
                upload_tbls()
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
