import logging
from operator import itemgetter
from typing import List, Tuple, Any, Dict, Mapping, Optional, Pattern, Union
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
    result = self.path_template
    for key, value in replacements.items():
      replace = type(self).delimiters[0] + key + type(self).delimiters[1]
      result = result.replace(replace, value)
    return result

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
    else:
      raise NotImplementedError


class PathTemplateInputSpec(InputSpec):
  """An input specified by a glob expression ('some/file/*/path.ext')"""

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


# Output Specification

class OutputSpec(ABC):
  """Output Specification Interface"""

  @abstractmethod
  def output_path(self, input_replacements: Mapping[str, str]) -> str:
    pass

  # @abstractmethod
  # def verify_placeholders(self, input_names: List[str]) -> bool:
  #   pass


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

  # def verify_placeholders(self, input_names: List[str]) -> bool:
  #   return set(input_names) == set(self.placeholders)

  def output_path(self, replacements: Mapping[str, str]) -> str:
    return self.path_template.format(replacements)


# Task Specification

class TaskSpec(object):
  """Data object describing which inputs a task expects."""

  def __init__(self, inputs: List[Tuple[str, object]], output: object,
               path: str, name: str) -> None:
    self.input_specs = [InputSpecFactory.build(input) for input in inputs]
    self.output_spec = OutputSpecFactory.build(output)
    # self._verify_placeholders()
    self.path = path
    self.name = name

  def __repr__(self) -> str:
    ios = self.input_specs + [self.output_spec] # ios: List[Union[InputSpec, OutputSpec]]
    reprs = ", ".join([repr(input) for input in ios])
    return "<TaskSpec {}, ({})>".format(self.name, reprs)

  # def _verify_placeholders_match(self) -> bool:
  #   output_placeholders = self.output_spec.placeholders

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
