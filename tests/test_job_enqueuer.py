import json

from flow.queue import LocalEnqueuer, GCTasksEnqueuer
from flow import io_adapter
from flow.io_adapter import GCStorageAdapter
import logging

from absl import flags
FLAGS = flags.FLAGS
FLAGS.mark_as_parsed()

# def test_job_enqueuer():
#   object = dict(test="test1.txt")
#   payload = json.dumps(object)
#   result = create_task(payload)
#   assert result

def test_local_job_enqueuer(noop_job_spec):
  noop_job_spec.task_path = "simulator/playground" + noop_job_spec.task_path
  enqueuer = LocalEnqueuer()
  enqueuer.add([noop_job_spec])


io_adapter.io = GCStorageAdapter()
def test_remote_job_enqueuer(noop_job_spec):
  enqueuer = GCTasksEnqueuer()
  enqueuer.add([noop_job_spec])
