""" FileEventHandlerAdapterEventHandler
https://www.extremetech.com/wp-content/uploads/2016/11/Dongles.png

Sounds messy; all it does is act as an adapter from the type of "EventHandler"
the "watchdog" library asks for to the flow interface "file_event_handler".
"""

import logging
from os import path
from watchdog.events import FileSystemEventHandler
from flow.event_handler import FileEventHandler
from flow.io_adapter import io

from pathlib import PurePath


class FileEventHandlerAdapterEventHandler(FileSystemEventHandler):
  """Redirects events to the task_handler API."""

  def __init__(self, root_dir: str) -> None:
    self.root_path = PurePath(root_dir)
    io.root_dir = root_dir
    self.handler = FileEventHandler()

  def on_moved(self, event):
    if not event.is_directory:
      logging.debug("TaskHandlerAdapterEventHandler : on_moved(), %s", event)
      pure_path = PurePath(event.src_path)
      relative = pure_path.relative_to(self.root_path)
      self.handler.handle_file_event('/' + str(relative))

  def on_created(self, event):
    if not event.is_directory:
      logging.debug("TaskHandlerAdapterEventHandler : on_created(), %s", event)
      pure_path = PurePath(event.src_path)
      relative = pure_path.relative_to(self.root_path)
      self.handler.handle_file_event('/' + str(relative))
