import uuid
import logging
from os.path import basename, exists
from typing import Dict, Tuple, Any
# from collections import OrderedDict

import inspect

from flow.io_adapter import io
from flow.task_spec import TaskSpec, InputSpec, OutputSpec
from flow.dynamic_import import import_module_from_local_source

RESERVED_NAMES = ['main', 'output', 'load', 'save', 'read', 'write', 'show']

class TaskParseError(Exception):
  pass


class TaskParser(object):
  """Given a file path, parses a task into a TaskSpec."""

  def __init__(self, task_path: str) -> None:
    if task_path is None:
      raise TaskParseError("task_path can not be None.")
    # TODO: think of better verification strategy?

    self.task_path = task_path

    if not exists(task_path):
      task_path = io.download(task_path)
    task_module = import_module_from_local_source(task_path)

    try:
      self.main_function = task_module.main # type: ignore
    except AttributeError:
      raise TaskParseError("Specified task ('{}') does not contain required method 'main'.".format(task_path))

    try:
      self.output_object = task_module.output # type: ignore
    except AttributeError:
      raise TaskParseError("Specified task ('{}') does not contain required attribute 'output'.".format(task_path))

    members = inspect.getmembers(task_module)
    inputs = list(filter(isinput, members))
    if not inputs:
      raise TaskParseError("Specified task ('{}') does not contain any inputs.".format(task_path))
    else:
      self.input_objects = inputs

    logging.debug("Successfully parsed task '{}'".format(task_path))

  def to_spec(self) -> TaskSpec:
    input_specs = [InputSpec.build(input) for input in self.input_objects]
    output_spec = OutputSpec.build(self.output_object)
    return TaskSpec(input_specs, output_spec, self.task_path, basename(self.task_path))


def isinput(tuple: Tuple[str, object]) -> bool:
  name, _ = tuple
  is_builtin = name.startswith('__')
  is_reserved = name in RESERVED_NAMES
  return not (is_builtin or is_reserved)
