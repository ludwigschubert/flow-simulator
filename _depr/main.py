import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

from adapters import task_handler_adapter

WATCH_PATH = 'playground'

if __name__ == "__main__":
  logging.basicConfig(level=logging.DEBUG,
                      format="[%(filename)25s:%(lineno)3s - %(funcName)25s() ] %(message)s",
                      datefmt='%Y-%m-%d %H:%M:%S')
  event_handler = task_handler_adapter.TaskHandlerAdapterEventHandler()
  observer = Observer()
  observer.schedule(event_handler, WATCH_PATH, recursive=True)
  observer.start()
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    observer.stop()
  observer.join()
