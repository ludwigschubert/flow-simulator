import logging
from typing import cast, List, Any
from abc import ABC, abstractmethod
import base64
import datetime
import json

import googleapiclient.discovery

from flow.queue.enqueuer import Enqueuer
from flow.job_spec import JobSpec
from flow.io_adapter import io

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
    paths = [job_spec.output for job_spec in job_specs]
    exist = io.exist(paths)
    for job_spec, exists in zip(job_specs, exist):
      if exists:
        logging.info("Skipping enqueueing %s because its output file already exists!", job_spec)
      else:
        json_payload = job_spec.to_json()
        response = self._create_task(json_payload)
        logging.info('Created task %s', response['name'])
