import logging
from typing import cast, List, Any
from abc import ABC, abstractmethod
from os import path, makedirs

from flow.queue.enqueuer import Enqueuer
from flow.job_spec import JobSpec
from flow.io_adapter import io

from absl import flags
FLAGS = flags.FLAGS
flags.DEFINE_string('local_queue_export_path', None, 'If supplied, save JobSpecs to this folder rather than executing them immediately.')
flags.DEFINE_boolean('local_queue_skip_exists_check', False, 'Whether to skip looking for already existing job outputs before deciding whether to run or export the jobspec.')

class LocalEnqueuer(Enqueuer):

  def add(self, job_specs: List[JobSpec]) -> None:
    if FLAGS.local_queue_export_path:
      makedirs(FLAGS.local_queue_export_path, exist_ok=True)

    for i, job_spec in enumerate(job_specs):
      if not FLAGS.local_queue_skip_exists_check and io.exists(job_spec.output):
        # TODO: we may need to do more than check for a single file
        # TODO: we may want to check for timestamps here?
        # TODO: we may want to support deleting files in events as a mechanism to triogger re-running? or not?
        logging.info("Skipping enqueueing %s because its output file already exists!", job_spec)
        continue
      else:
        serialized = job_spec.to_json(pretty=True)
        if FLAGS.local_queue_export_path:
          with open(path.join(FLAGS.local_queue_export_path, "job_spec_{i:05}.json".format(i=i)), 'w') as handle:
            handle.write(serialized)
        else:
          deserialized = JobSpec.from_json(serialized)
          deserialized.execute()
