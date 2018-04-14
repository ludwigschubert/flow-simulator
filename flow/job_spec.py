import logging
from typing import List, Tuple, Any, Dict
import json as JSON
from imp import load_source
from os.path import basename, splitext

from flow.io_adapter import io
from flow.dynamic_import import import_module_from_local_source

class JobSpec(object):
  """Serializable data object describing which task to execute and its inputs."""

  def __init__(self, inputs: List[Tuple[str, str]], output: str, task_path: str) -> None:
    self.inputs = inputs
    self.output = output
    self.task_path = task_path

  def __eq__(self, other: object) -> bool:
    if isinstance(self, other.__class__):
        return self.__dict__ == other.__dict__
    return False

  def __repr__(self) -> str:
    name = basename(self.task_path)
    return "<JobSpec {}, ({})-> {}>".format(name, self.inputs, self.output)

  @classmethod
  def value_for_input(cls, input: object) -> object:
    if isinstance(input, str):
      # TODO: check if is valid path!!!
      # TODO: move to loaders!!!
      if splitext(input)[1] == '.txt':
        with io.reading(input) as input_file:
          value = input_file.read().decode().rstrip('\n')
        return value
      else:
        return input
    if isinstance(input, (int, float, tuple, list, dict, set)):
      # TODO: how to transform??
      return input
    else:
      raise NotImplemented

  @classmethod
  def value_for_output(cls, output: str) -> str:
    if isinstance(output, str):
      return output
    else:
      raise NotImplemented

  @classmethod
  def save_result_for_output(cls, result: object, output: object) -> None:
    if result is None:
      logging.info("Task did not return a result; but maybe it's just saving the result itself?")
      return
    if isinstance(output, str):
      # assume path for now
      if isinstance(result, str):
        with io.writing(output) as output_file:
          output_file.write(result.encode())
        # TODO: loaders and savers? Assume serialized already for now.
      else:
        raise NotImplemented
    else:
      raise NotImplemented

  def execute(self) -> None:
    # load module
    local_task_path = io.download(self.task_path)
    module = import_module_from_local_source(local_task_path)
    # set inputs
    for name, input in self.inputs:
      value = self.value_for_input(input)
      logging.debug("Setting '%s' to '%s' in module '%s'", name, value, module)
      setattr(module, name, value)
    # set output; e.g. in case the task saves its own results
    output_value = self.value_for_output(self.output)
    logging.debug("Setting 'output' to '%s' in module '%s'", value, module)
    setattr(module, 'output', output_value)
    # execute and save result
    self.result = module.main() # type: ignore
    self.save_result_for_output(self.result, self.output)
    # unload module
    # TODO: test if that actually allows us to call this method multiple times!
    del module


  # Serialization

  @classmethod
  def from_json(cls, json: str):
    dict = JSON.loads(json)
    dict['inputs'] = [(key, value) for [key, value] in dict['inputs']]
    return JobSpec(**dict)

  def to_json(self) -> str:
    return JSON.dumps(self.__dict__)
