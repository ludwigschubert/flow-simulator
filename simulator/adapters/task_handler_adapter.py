import logging
from watchdog.events import FileSystemEventHandler
import task_handler


class TaskHandlerAdapterEventHandler(FileSystemEventHandler):
  """Redirects events to the task_handler API."""
  def on_moved(self, event):
    if not event.is_directory:
      logging.debug("TaskHandlerAdapterEventHandler : on_moved(), %s", event)
      task_handler.handle_task(event)

  def on_created(self, event):
    if not event.is_directory:
      logging.debug("TaskHandlerAdapterEventHandler : on_created(), %s", event)
      task_handler.handle_task(event)
