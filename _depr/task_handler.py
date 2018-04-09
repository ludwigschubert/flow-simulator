import logging

# regex-lite ala unix globs
from glob import glob
from fnmatch import fnmatch

# dynamic import and inspection
import imp
import inspect

from itertools import product
from functools import partial

tasks_glob = 'playground/tasks/*.py'

# {src_path} -> ()
def handle_task(event):
  logging.info('handling file event: %s', event.src_path)
  logging.debug("event %s", event)
  src_path = event.src_path
  is_task = fnmatch(src_path, tasks_glob)
  if is_task:
    handle_new_task(src_path)
  else:
    handle_new_input(src_path)


def handle_new_task(task_spec):
  for handler in task_handlers([task_spec]):
    create_jobs(handler)  # no src_path!

def handle_new_input(src_path):
  task_specs = glob(tasks_glob)
  for handler in task_handlers(task_specs):
    if is_relevant(handler, src_path):
      logging.info('found relevant handler %s for file %s', handler.__name__, src_path)
      create_jobs(handler, src_path)
    else:
      logging.debug('skipped handler %s for file %s', handler.__name__, src_path)


# [path] -> [function]
def task_handlers(task_specs):
  handlers = []
  for i, task_spec in enumerate(task_specs):
    # TODO: do we *want* a unique, random name? do we care?
    task_module_name = 'task_specification_' + str(i)
    task_module = imp.load_source(task_module_name, task_spec)
    logging.debug("task_module, %s", dir(task_module))
    task_module_functions = inspect.getmembers(task_module, inspect.isfunction)
    logging.debug("task_module_functions, %s", task_module_functions)
    logging.debug("greeting?, %s", task_module.greeting)
    logging.debug("greeting inputs?, %s", task_module.greeting.input_specs)

    for function_name, function in task_module_functions:
      if getattr(function, 'is_task_handler', False):
        handlers.append(function)

  return handlers


def input_name_matching_path(input_specs, src_path):
  for input_name, input_glob in input_specs:
    if fnmatch(src_path, input_glob):
      return input_name
  return None


def is_relevant(handler, src_path):
  potential_match = input_name_matching_path(handler.input_specs, src_path)
  return potential_match != None


def create_jobs(handler, src_path=None):
  """Adds all new jobs for thsi handler.

  If no `src_path` is supplied, assumes the handler itself is new and adds all
  possible jobs for it.
  """
  input_specs = handler.input_specs

  if src_path:
    supplied_input_name = input_name_matching_path(input_specs, src_path)
  else:
    supplied_input_name = None

  inputs = []
  for input_name, input_glob in input_specs:
    logging.debug("%s, %s, %s", supplied_input_name, input_name, input_glob)
    if input_name == supplied_input_name:
      inputs.append([src_path])
    else:
      inputs.append(sorted(glob(input_glob)))
  logging.debug("inputs: %s", inputs)

  for args in product(*inputs):
    # we could pass by position, but it feels cleaner to go by keyword args
    assert len(args) == len(input_specs)
    kwargs = dict(zip([name for name, _ in input_specs], args))
    job = partial(handler, **kwargs)  # capture args in closure
    job.output_spec = handler.output_spec
    # there will need to be serialization here sometime…
    add_job(job)


def add_job(job):
  """Currently faked by simply executing it."""
  job()
  logging.debug(job.output_spec)
