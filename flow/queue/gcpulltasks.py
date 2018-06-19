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
from flow.util import batch


class GCPullTasksEnqueuer(Enqueuer):
    def __init__(
        self,
        project: str = "brain-deepviz",
        location: str = "us-central1",
        queue: str = "flow-jobs-pull",
    ) -> None:
        self.project = project
        self.location = location
        self.queue = queue
        self.client = googleapiclient.discovery.build(
            "cloudtasks", "v2beta2", cache_discovery=False
        )

    @property
    def queue_name(self) -> str:
        return f"projects/{self.project}/locations/{self.location}/queues/{self.queue}"

    def add(self, job_specs: List[JobSpec]) -> None:
        for job_spec_batch in batch(job_specs, 1000):
            batch_request = self.client.new_batch_http_request()
            # TODO: re-run iff output timestamp is older than task's?
            for job_spec in job_spec_batch:
                # uses fast in-memory file list lookup
                if io.exists(job_spec.output):
                    continue
                payload = job_spec.to_json()
                base64_encoded_payload = base64.b64encode(payload.encode())
                converted_payload = base64_encoded_payload.decode()
                body = {"task": {"pullMessage": {"payload": converted_payload}}}
                request = (
                    self.client.projects()
                    .locations()
                    .queues()
                    .tasks()
                    .create(parent=self.queue_name, body=body)
                )
                batch_request.add(request)
            batch_request.execute()
