from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from ingestao import execute

class FileCreateHandler(FileSystemEventHandler):
    def on_created(self, event):
        execute()
        print("File created.")

if __name__ == "__main__":

    event_handler = FileCreateHandler()

    # Create an observer.
    observer = Observer()

    # Attach the observer to the event handler.
    observer.schedule(event_handler, "..\DataLakers\Mr_Health\csv-tables", recursive=True)

    # Start the observer.
    observer.start()

    try:
        while observer.is_alive():
            observer.join(1)
    finally:
        observer.stop()
        observer.join()