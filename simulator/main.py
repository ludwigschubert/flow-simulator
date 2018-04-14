"""
Running this is awkward because the simulator depends on flow, but not explicitly.
Run from root directory and specify that directory as python path:

```bash
PYTHONPATH='.' python simulator/main.py
```
"""

import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

from file_event_handler_adapter import FileEventHandlerAdapterEventHandler


WATCH_PATH = 'simulator/playground'

if __name__ == "__main__":
  event_handler = FileEventHandlerAdapterEventHandler(root_dir=WATCH_PATH)
  observer = Observer()
  observer.schedule(event_handler, WATCH_PATH, recursive=True)
  observer.start()
  try:
    while True:
      time.sleep(1)
  except KeyboardInterrupt:
    observer.stop()
  observer.join()
