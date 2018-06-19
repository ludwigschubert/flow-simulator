import logging

from google.cloud import pubsub
from typing import List, Any

from flow.queue.enqueuer import Enqueuer
# from flow.job_spec import JobSpec

class GCPubSubEnqueuer(Enqueuer):

  def __init__(self, project: str = 'brain-deepviz',
                     topic: str = 'flow-jobs') -> None:
    self.project = project
    self.topic = topic
    self.client = pubsub.PublisherClient()

  def add(self, job_specs: List[Any]) -> None:
    topic = f'projects/{self.project}/topics/{self.topic}'
    for job_spec in job_specs:
      message = job_spec.to_json().encode()
      self.client.publish(topic, message)
