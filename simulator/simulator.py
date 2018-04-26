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
import builtins

from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_boolean('watch', False, 'Watch a directory.')
flags.DEFINE_boolean('use_local_queue', False, 'Use local queue simulator instead of remote Google Cloud Tasks queue.')
flags.DEFINE_string('watch_path', 'simulator/playground', 'Relative path to directory that should be watched for file system events.')
flags.DEFINE_string('preflight_task', None, 'List JobSpecs that a task could enqueue.')


from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler

from file_event_handler_adapter import FileEventHandlerAdapterEventHandler
from flow.task_parser import TaskParser
from flow.task_spec import TaskSpec


def main(argv):
  del argv  # Unused.

  if FLAGS.preflight_task:
    src_path = FLAGS.preflight_task
    parser = TaskParser(src_path)
    task_spec = parser.to_spec()
    logging.info("Task parsed successfully: %s", task_spec)
    logging.info("Input dependencies: %s", task_spec.dependencies)
    job_specs = task_spec.to_job_specs()
    logging.info("Task %s could create %d jobs:", task_spec.name, len(job_specs))
    for job_spec in job_specs:
      logging.info(job_spec)

  elif FLAGS.watch:
    event_handler = FileEventHandlerAdapterEventHandler(root_dir=FLAGS.watch_path)
    observer = Observer()
    observer.schedule(event_handler, FLAGS.watch_path, recursive=True)
    observer.start()
    logging.info("Simulator ready, observing path '%s'", FLAGS.watch_path)
    try:
      while True:
        time.sleep(1)
    except KeyboardInterrupt:
      observer.stop()
    observer.join()

  else:
    logging.fatal("No command given, see usage, etc TODO")

if __name__ == '__main__':
  app.run(main)
