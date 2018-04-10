import uuid
import logging
from typing import Dict, Tuple, Any
# from collections import OrderedDict

import imp
import inspect

from flow.io_adapter import exists
from flow.task_spec import TaskSpec


MAIN_NAME = 'main'
OUTPUT_NAME = 'output'


class TaskParser(object):
  """Given a file path, parses a task into a TaskSpec."""

  def __init__(self, task_path: str) -> None:
    if task_path is None:
      raise ValueError("task_path can not be None.")
    if not exists(task_path):
      raise ValueError("task_path ('{}') does not exist".format(task_path))
    task_module_name = 'task_specification_' + str(uuid.uuid4())
    task_module = imp.load_source(task_module_name, task_path)

    try:
      self.main_function = task_module.main # type: ignore
    except AttributeError:
      raise ValueError("Specified task ('{}') does not contain required method 'main'.".format(task_path))

    try:
      self.output_object = task_module.output # type: ignore
    except AttributeError:
      raise ValueError("Specified task ('{}') does not contain required attribute 'output'.".format(task_path))

    members = inspect.getmembers(task_module)
    inputs = list(filter(isinput, members))
    if not inputs:
      raise ValueError("Specified task ('{}') does not contain any inputs.".format(task_path))
    else:
      self.input_objects = inputs

    self.task_path = task_path

    logging.debug("Successfully parsed task '{}'".format(task_path))

  def to_task_spec(self) -> TaskSpec:
    return TaskSpec(self.input_objects, self.output_object, self.task_path)


def isinput(tuple: Tuple[str, object]) -> bool:
  name = tuple[0]
  is_builtin = name.startswith('__')
  is_output = name == OUTPUT_NAME
  is_main = name == MAIN_NAME
  return not (is_builtin or is_output or is_main)
