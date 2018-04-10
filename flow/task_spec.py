from operator import itemgetter
from typing import List, Tuple, Any, Dict
import re


class InputSpec(object):
  """Given an input spec."""

  def __init__(self, inputs: List[Tuple[str, object]]) -> None:
    self.inputs = inputs

  @property
  def names(self) -> List[str]:
    return [name for name, _ in self.inputs]


class OutputSpec(object):
  """Given an output spec, creates paths etc"""

  placeholder_pattern = re.compile(r"{([^}]+)}")

  def __init__(self, output: object) -> None:
    self.output = output
    if isinstance(output, str):
      self.output_names = self.placeholder_pattern.findall(output) # type: List[str]
    else:
      raise NotImplemented

  def verify_placeholders(self, input_names: List[str]) -> bool:
    return set(input_names) == set(self.output_names)

  def output_path(self, input_replacements: Dict[str, str]) -> str:
    #TODO: replace with these values once testing works
    pass


class TaskSpec(object):
  """Data object describing which inputs a task expects."""

  def __init__(self, inputs: List[Tuple[str, object]], output: object, path: str) -> None:
    self.input_spec = InputSpec(inputs)
    self.output_spec = OutputSpec(output)
    self.output_spec.verify_placeholders(self.input_spec.names)
    self.path = path
