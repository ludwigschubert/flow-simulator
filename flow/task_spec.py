from operator import itemgetter
from typing import List, Tuple, Any, Dict
import re


class InputSpec(object):
  """Given an input spec."""

  def __init__(self, inputs: List[Tuple[str, Any]]) -> None:
    self.inputs = inputs

  @property
  def names(self):
    return list(map(itemgetter(0),  self.inputs))


class OutputSpec(object):
  """Given an output spec, creates paths etc"""

  placeholder_pattern = re.compile(r"{([^}]+)}")

  def __init__(self, output: Any):
    self.output = output
    self.output_names = self.placeholder_pattern.findall(output)

  def verify_placeholders(self, input_names: List[str]):
    return set(input_names) == set(self.output_names)

  def output_path(self, input_replacements: Dict[str, str]):
    #TODO: replace with these values once testing works
    pass


class TaskSpec(object):
  """Data object describing which inputs a task expects."""

  def __init__(self, inputs: List[Tuple[str, Any]], output: Any, path: str) -> None:
    self.input_spec = InputSpec(inputs)
    self.output_spec = OutputSpec(output)
    self.output_spec.verify_placeholders(self.input_spec.names)
    self.path = path
