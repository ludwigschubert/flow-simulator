import logging
from typing import List, Tuple, Any, Dict
import json as JSON
from imp import load_source
from flow.io_adapter import open_file

# TODO: potentially refactor into a job_handler that only executes the job_spec

class JobSpec(object):
  """Serializable data object describing which task to execute and its inputs."""

  def __init__(self, inputs: List[Tuple[str, Any]], output: Any, path: str) -> None:
    self.inputs = inputs # type: MutableMapping[str, str]
    self.output = output
    self.path = path

  def __eq__(self, other):
    logging.warn("Other __dict__: %s", other.__dict__)
    logging.warn("Own __dict__: %s", self.__dict__)
    if isinstance(self, other.__class__):
        return self.__dict__ == other.__dict__
    return False

  @classmethod
  def value_for_input(cls, input: Any):
    if isinstance(input, str):
      # TODO: load values, etc?
      logging.warn("TODO: got string; we should parse and check for path.")
      return input
    if isinstance(input, (int, float, tuple, list, dict, set)):
      # TODO: how to transform??
      return input
    else:
      raise NotImplemented

  @classmethod
  def value_for_output(cls, output: Any):
    if isinstance(output, str):
      return output
    else:
      raise NotImplemented

  @classmethod
  def save_result_for_output(cls, result: Any, output: Any):
    if result is None:
      logging.info("Task did not return a result; maybe it's just saving the result itself?")
      return
    if isinstance(output, str):
      # assume path for now
      with open_file(output, 'w') as output_file:
        output_file.write(result)
      # TODO: loaders and savers? Assume serialized already for now.

    else:
      raise NotImplemented

  def execute(self):
    # load module
    module = load_source('job_module', self.path)
    # set inputs
    for name, input in self.inputs:
      value = self.value_for_input(input)
      logging.warn("Setting '%s' to '%s' in module '%s'", name, value, module)
      setattr(module, name, value)
    # set output; e.g. in case the task saves its own results
    output_value = self.value_for_output(self.output)
    setattr(module, 'output', output_value)
    # execute and save result
    self.result = module.main()
    self.save_result_for_output(self.result, self.output)
    # unload module
    # TODO: test if that actually allows us to call this method multiple times!
    del module


  # Serialization

  @classmethod
  def from_json(cls, json):
    dict = JSON.loads(json)
    dict['inputs'] = [(key, value) for [key, value] in dict['inputs']]
    return JobSpec(**dict)

  def to_json(self):
    return JSON.dumps(self.__dict__)
