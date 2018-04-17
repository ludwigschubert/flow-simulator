import logging
from operator import itemgetter
import collections
from typing import List, Tuple, Any, Dict, Mapping, Optional, Pattern, Union, Iterable
import re
from abc import ABC, abstractmethod
from os.path import basename, splitext
import fnmatch

from flow.io_adapter import io


class PathTemplateError(Exception):
  pass

class PathTemplate(object):

  delimiters = ('{', '}')
  pattern = re.compile(r"{([^}]+)}")

  def __init__(self, path_template: str) -> None:
    if "*" in path_template:
      raise PathTemplateError("Path template ("+path_template+") may not contain '*'. Use {name} instead.")
    self.path_template = path_template

  def __repr__(self) -> str:
    return "<PathTemplate {}>".format(self.path_template)

  @property
  def _capture_regex(self) -> Pattern[str]:
    regex = self.path_template \
      .replace("/", "\/") \
      .replace("{", r"(?P<") \
      .replace("}", r">[^{}/]+)")
    return re.compile(regex)

  @property
  def glob(self) -> str:
    replacements = dict((placeholder, '*') for placeholder in self.placeholders)
    return self.format(replacements)

  @property
  def placeholders(self) -> List[str]:
    return self.pattern.findall(self.path_template)

  def format(self, replacements: Mapping[str, str]) -> str:
    return self.path_template.format(**replacements)

  def match(self, path: str) -> Optional[Dict[str, str]]:
    match = self._capture_regex.match(str(path))
    if match:
      return match.groupdict()
    else:
      return None


class InputSpec(ABC):
  """Input Specification Interface"""

  name: str # https://www.python.org/dev/peps/pep-0526/

  @abstractmethod
  def matches(self, src_path: str) -> bool:
    pass

  @abstractmethod
  def values(self, path: Optional[str] = None) -> List[Tuple[str, Mapping[str, str]]]:
    pass


class InputSpecFactory(object):
  """InputSpec Factory"""

  @classmethod
  def build(cls, input: Tuple[str, object]) -> InputSpec:
    name, descriptor = input
    if isinstance(descriptor, str):
      # TODO: check if is path? leading '/'?
      path_template = PathTemplate(descriptor)
      return PathTemplateInputSpec(name, path_template)
    elif isinstance(descriptor, collections.Iterable):
      return IterableInputSpec(name, descriptor)
    else:
      raise NotImplementedError


class PathTemplateInputSpec(InputSpec):
  """An input specified by a glob expression ('some/file/*/path.ext')."""

  def __init__(self, name: str, path_template: PathTemplate) -> None:
    self.name = name
    self.path_template = path_template

  def __repr__(self) -> str:
    return "In: {}".format(self.path_template)

  def matches(self, src_path: str) -> bool:
    return self.path_template.match(src_path) is not None

  def values(self, path: Optional[str] = None) -> List[Tuple[str, Mapping[str, str]]]:
    if path:
      paths = [path]
    else:
      glob = self.path_template.glob
      paths = io.glob(glob)
    return [(path, self.path_template.match(path)) for path in paths]


class IterableInputSpec(InputSpec):
  """An input specified by an iterable object such as a list."""

  def __init__(self, name: str, iterable: Iterable) -> None:
    self.name = name
    self.iterable = iterable

  def __repr__(self) -> str:
    return "In: {}".format(list(self.iterable))

  def matches(self, src_path: str) -> bool:
    return False

  def values(self, *args: Any) -> List[Tuple[str, Mapping[str, str]]]:
    # TODO: this needs to be clarified
    # TODO: this value to basename(value) business needs to be generalized.
    return [(value, dict([(self.name, basename(value))])) for value in self.iterable]


# Output Specification

class OutputSpec(ABC):
  """Output Specification Interface"""

  @abstractmethod
  def output_path(self, input_replacements: Mapping[str, str]) -> str:
    pass

  @property
  @abstractmethod
  def placeholders(self) -> List[str]:
    pass


class OutputSpecFactory(object):
  """OutputSpec Factory"""

  @classmethod
  def build(cls, descriptor: object) -> OutputSpec:
    if isinstance(descriptor, str):
      path_template = PathTemplate(descriptor)
      return PathTemplateOutputSpec(path_template)
    else:
      raise NotImplementedError


class PathTemplateOutputSpec(OutputSpec):
  """Given an output spec, creates paths etc"""

  placeholder_pattern = re.compile(r"{([^}]+)}")

  def __init__(self, path_template: PathTemplate) -> None:
    self.path_template = path_template

  def __repr__(self) -> str:
    return "Out: {}".format(self.path_template)

  @property
  def placeholders(self) -> List[str]:
    return self.path_template.placeholders

  def output_path(self, replacements: Mapping[str, str]) -> str:
    return self.path_template.format(replacements)


# Task Specification

class TaskSpec(object):
  """Data object describing which inputs a task expects."""

  def __init__(self, inputs: List[Tuple[str, object]], output: object,
               src_path: str, name: str) -> None:
    self.src_path = src_path
    self.name = name
    self.input_specs = [InputSpecFactory.build(input) for input in inputs]
    self.output_spec = OutputSpecFactory.build(output)
    self._verify_placeholders()

  def __repr__(self) -> str:
    ios = self.input_specs + [self.output_spec] # ios: List[Union[InputSpec, OutputSpec]]
    reprs = ", ".join([repr(input) for input in ios])
    return "<TaskSpec {}, ({})>".format(self.name, reprs)

  def _verify_placeholders(self) -> None:
    outputs = set(self.output_spec.placeholders)
    inputs = set(input_spec.name for input_spec in self.input_specs)
    if not outputs == inputs:
      if outputs.issuperset(inputs):
        difference = outputs - inputs
        raise ValueError("Placeholders '{}' in task_spec '{}' do not have input variables that could replace them.".format(difference, self.name))
      else: # TODO: this covers both subset and entirely disjoint. is the error message bringing that across? no.
        difference = inputs - outputs
        raise ValueError("Input variables '{}' in task_spec '{}' do not have any corresponding output placeholders.".format(difference, self.name))

  task_specification_glob = '/tasks/*.py'

  @classmethod
  def is_task_path(cls, path: str) -> bool:
    # right_folder = path.startswith('/tasks/')
    # right_ext = splitext(path) == '.py'
    # return right_folder and right_ext
    return fnmatch.fnmatch(path, cls.task_specification_glob)

  @property
  def input_names(self) -> List[str]:
    return [input_spec.name for input_spec in self.input_specs]

  def matching_input_spec(self, src_path: str) -> Optional[InputSpec]:
    for input_spec in self.input_specs:
      if input_spec.matches(src_path):
        return input_spec
    return None

  def should_handle_file(self, src_path: str) -> bool:
    return self.matching_input_spec(src_path) is not None
