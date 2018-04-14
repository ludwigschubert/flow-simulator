import uuid
import logging
from os.path import basename
from typing import Dict, Tuple, Any
# from collections import OrderedDict

import inspect

from flow.io_adapter import io
from flow.task_spec import TaskSpec
from flow.dynamic_import import import_module_from_local_source


MAIN_NAME = 'main'
OUTPUT_NAME = 'output'


class TaskParseError(Exception):
  pass


class TaskParser(object):
  """Given a file path, parses a task into a TaskSpec."""

  def __init__(self, task_path: str) -> None:
    if task_path is None:
      raise TaskParseError("task_path can not be None.")
    # TODO: think of better verification strategy?

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

    self.task_path = task_path

    logging.debug("Successfully parsed task '{}'".format(task_path))

  def to_spec(self) -> TaskSpec:
    name = basename(self.task_path)
    return TaskSpec(self.input_objects, self.output_object, self.task_path, name)


def isinput(tuple: Tuple[str, object]) -> bool:
  name = tuple[0]
  is_builtin = name.startswith('__')
  is_output = name == OUTPUT_NAME
  is_main = name == MAIN_NAME
  return not (is_builtin or is_output or is_main)
