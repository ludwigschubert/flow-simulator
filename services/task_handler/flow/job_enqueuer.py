import logging
from typing import cast, List, Any
from abc import ABC, abstractmethod
from flow.job_spec import JobSpec
from flow.io_adapter import io


class Enqueuer(ABC):

  @abstractmethod
  def add(self, job_specs: List[JobSpec]) -> None:
    pass


# LocalEnqueuer

class LocalEnqueuer(Enqueuer):

  def add(self, job_specs: List[JobSpec]) -> None:
    for job_spec in job_specs:
      if io.exists(job_spec.output):
        # TODO: we may need to do more than check for a single file
        # TODO: we may want to check for timestamps here?
        # TODO: we may want to support deleting files in events as a mechanism to triogger re-running? or not?
        logging.info("Skipping enqueueing %s because its output file already exists!", job_spec)
      else:
        serialized = job_spec.to_json()
        deserialized = JobSpec.from_json(serialized)
        deserialized.execute()



# GCTasksEnqueuer

import googleapiclient.discovery
import base64
import datetime
import json

class GCTasksEnqueuer(Enqueuer):

  def __init__(self, project: str = 'brain-deepviz',
                     location: str = 'us-central1',
                     queue: str = 'flow-jobs',
                     service_url: str = '/handle_job') -> None:
    self.project = project
    self.location = location
    self.queue = queue
    self.service_url = service_url
    self.client = googleapiclient.discovery.build('cloudtasks', 'v2beta2', cache_discovery=False)

  @property
  def queue_name(self) -> str:
    return 'projects/{}/locations/{}/queues/{}'.format(self.project, self.location, self.queue)

  def _create_task(self, payload: str) -> Any:
    base64_encoded_payload = base64.b64encode(payload.encode())
    converted_payload = base64_encoded_payload.decode()
    body = {'task': {'appEngineHttpRequest': {
                'httpMethod': 'POST',
                'relativeUrl': self.service_url,
                'payload': converted_payload
            }}}
    response = self.client.projects().locations().queues().tasks().create(
        parent=self.queue_name, body=body).execute()
    return response

  def add(self, job_specs: List[JobSpec]) -> None:
    for job_spec in job_specs:
      if io.exists(job_spec.output):
        logging.info("Skipping enqueueing %s because its output file already exists!", job_spec)
      else:
        json_payload = job_spec.to_json()
        response = self._create_task(json_payload)
        logging.info('Created task %s', response['name'])


from os import getenv
enqueuer : Enqueuer # don't leak concrete type information outside of module!
if getenv('USE_LOCAL_QUEUE', '').startswith('TRUE'):
  logging.warn("Using LocalEnqueuer!")
  enqueuer = LocalEnqueuer()
else:
  enqueuer = GCTasksEnqueuer()
