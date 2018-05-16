from typing import cast, List, Any
from abc import ABC, abstractmethod

from flow.job_spec import JobSpec

class Enqueuer(ABC):

  @abstractmethod
  def add(self, job_specs: List[JobSpec]) -> None:
    pass
