from decorator import decorate
from decorator import getargspec
import logging
import re

output_placeholder_regex = re.compile(r"({[^}]+})")

def task(output):
  logging.debug("flow.task, output_spec: %s", output)
  def decorator(handler, *args, **kwargs):
    logging.debug("flow.task decorator running")
    argspec = getargspec(handler)
    input_names = argspec.args
    input_specs = list(zip(argspec.args, argspec.defaults))
    logging.debug("input_specs: %s", input_specs)
    # TODO: is this a good place for this metadata? separate object? task_spec?
    # Ensure all input names are found in teh output template so inputs uniquely
    # specify where their results get saved.
    # TODO: does that work well with creating multiple files?
    # TODO: does that mean we need a separate input syntax for things like
    # neurons/channels that don't come from files?
    output_placeholders = output_placeholder_regex.findall(output)
    assert len(output_placeholders) == len(input_specs)
    output_names = [placeholder[1:-1] for placeholder in output_placeholders]
    assert set(output_names) == set(input_names)
    # TODO: these asserts shoud throw custom Exceptions explaining what's missing
    # e.g. "output_spec does not contain all input variable names as placeholders"
    logging.debug("%s", output_placeholders)
    handler.input_specs = input_specs
    handler.output_spec = output
    handler.is_task_handler = True
    return handler
  return decorator
