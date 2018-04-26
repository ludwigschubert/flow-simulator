import logging
from string import Template
from operator import itemgetter
from inspect import getargspec
import collections
from typing import NewType, List, Tuple, Any, Dict, Mapping, Optional, Pattern, Union, Iterable, Callable, Set, cast, Sequence
from re import compile as re_compile
from abc import ABC, abstractmethod
from os.path import basename, splitext
import fnmatch
from functools import reduce
from toposort import toposort, toposort_flatten
from json import dumps

from flow.typing import Bindings, Variable, Value
from flow.io_adapter import io
from flow.job_spec import JobSpec


# PathTemplate

class PathTemplateError(ValueError):
  pass

class PathTemplate(Template):

  delimiter = '{'
  pattern = r'{(?P<named>[^{}/]+)}'

  def __init__(self, path_template: str) -> None:
    if "*" in path_template:
      raise PathTemplateError("Path template ("+path_template+") may not contain '*'. Use {name} instead.")
    if not path_template.startswith('/'):
      raise PathTemplateError("Path template ("+path_template+") must start with '/'.")
    super().__init__(path_template)

  def __repr__(self) -> str:
    return "<PathTemplate {}>".format(self.template)

  @property
  def _capture_regex(self) -> Pattern[str]:
    regex = self.template \
      .replace("/", "\/") \
      .replace("{", r"(?P<") \
      .replace("}", r">[^{}/]+)")
    return re_compile(regex)

  @property
  def glob(self) -> str:
    substitution: Mapping[str, str] = collections.defaultdict(lambda: '*')
    return self.substitute(substitution)

  def format(self, replacements: Mapping[str, str]) -> str:
    return self.template.format(**replacements)

  @property
  def placeholders(self) -> List[Variable]:
    return self.pattern.findall(self.template)

  def with_replacements(self, replacements: Mapping[Variable, str]) -> 'PathTemplate':
    return PathTemplate(self.safe_substitute(replacements))

  def match(self, path: str) -> Optional[Dict[str, str]]:
    match = self._capture_regex.match(str(path))
    if match:
      return match.groupdict()
    else:
      return None


# Input & Output Spec

class Spec(ABC):
  """Abstract Superclass for InputSpec and OutputSpec"""
  # TODO: what moves here? declared variables at least?
  pass

  @abstractmethod
  def depends_on(self) -> Set[Variable]:
    pass


class InputSpecError(ValueError):
  pass


class InputSpec(Spec):
  """Input Specification Interface"""

  name: Variable

  @classmethod
  def build(cls, input: Tuple[str, object]) -> 'InputSpec':
    name, descriptor = input
    if isinstance(descriptor, str):
      path_template = PathTemplate(descriptor)
      return PathTemplateInputSpec(Variable(name), path_template)
    elif isinstance(descriptor, dict):
      return AggregatingInputSpec(Variable(name), descriptor)
    elif isinstance(descriptor, Sequence):
      return IterableInputSpec(Variable(name), descriptor)
    elif callable(descriptor):
      return DependentInputSpec(Variable(name), descriptor)
    else:
      logging.error(input)
      raise NotImplementedError

  @abstractmethod
  def matches(self, src_path: str) -> bool:
    pass

  @abstractmethod
  def implicitly_declared_variables(self) -> Set[Variable]:
    pass

  def declared_variables(self) -> Set[Variable]:
    variables = self.implicitly_declared_variables()
    variables.add(self.name)
    return variables

  @abstractmethod
  def values(self, variable: Variable, bindings: Bindings) -> Set[Value]:
    pass


class IterableInputSpec(InputSpec):
  """An input specified by an iterable object such as a list."""

  def __init__(self, name: str, iterable: Iterable[Value]) -> None:
    self.name = Variable(name)
    self.iterable = iterable

  def __repr__(self) -> str:
    return "IterIn: {}".format(list(self.iterable))

  def matches(self, src_path: str) -> bool:
    return False

  def depends_on(self) -> Set[Variable]:
    return set([]) #set([self.name])

  def implicitly_declared_variables(self) -> Set[Variable]:
    return set()

  def values(self, variable: Variable, bindings: Bindings) -> Set[Value]:
    assert variable == self.name
    if self.name in bindings:
      bound_value = bindings[self.name]
      if bound_value in self.iterable:
        return {bound_value}
      else:
        return set()
    else:
      return set(self.iterable)


class PathTemplateInputSpec(InputSpec):
  """An input specified by a glob expression ('some/file/*/path.ext')."""

  def __init__(self, name: str, path_template: PathTemplate) -> None:
    assert isinstance(path_template, PathTemplate)
    self.name = Variable(name)
    self.path_template = path_template
    if name in path_template.placeholders:
      raise InputSpecError('InputSpec <{}> declares same variable identifier that its path template {} declares: "{}"'.format(self, path_template, name))

  def __repr__(self) -> str:
    return "PathTmplIn: {}".format(self.path_template)

  def depends_on(self) -> Set[Variable]:
    return set(self.path_template.placeholders)

  def matches(self, src_path: str) -> bool:
    return self.path_template.match(src_path) is not None

  def implicitly_declared_variables(self) -> Set[Variable]:
    return set(self.path_template.placeholders)

  def values(self, variable: Variable, bindings: Bindings) -> Set[Value]:
    assert variable == self.name or variable in self.implicitly_declared_variables()
    if variable == self.name:
      return set([self.path_template.substitute(bindings)])
    else:
      glob_string = self.path_template.with_replacements(bindings).glob
      paths = io.glob(glob_string)
      values = set(self.path_template.match(path).get(variable, None) for path in paths)
      return values


  # def values(self, path: Optional[str] = None) -> List[Tuple[str, Mapping[str, str]]]:
  #   if path:
  #     paths = [path]
  #   else:
  #     glob = self.path_template.glob
  #     paths = io.glob(glob)
  #   return [(path, self.path_template.match(path)) for path in paths]


class AggregatingInputSpec(InputSpec):
  """An input specification that captures local variables from a PathTemplate"""

  def __init__(self, name: str, dictionary: Dict[Tuple[str, ...], str]) -> None:
    self.name = Variable(name)
    items = list(dictionary.items())
    if len(items) != 1:
      raise InputSpecError('AggregatingInputSpec only takes dictionaries with exactly one entry')
    keys, path_template_string = items[0]
    if not isinstance(keys, tuple): # special case because a == (a) != (a,)
      keys = [keys]
    invalid_keys = list(filter(self._is_invalid_key, keys))
    if invalid_keys:
      raise InputSpecError("AggregatingInputSpec keys {} should be enclosed in curly braces.".format(invalid_keys))
    self.locally_bound_variables = [key[1:-1] for key in keys]
    self.path_template = PathTemplate(path_template_string)
    if not set(self.locally_bound_variables).issubset(set(self.path_template.placeholders)):
      diff = set(self.locally_bound_variables) - set(self.path_template.placeholders)
      raise InputSpecError('AggregatingInputSpec declares variables {} which are not bound locally in its path template {}.'.format(diff, self.path_template))

  def __repr__(self) -> str:
    return "Aggr.In: {}, aggregates {}".format(self.path_template, self.locally_bound_variables)

  def matches(self, src_path: str) -> bool:
    return bool(self.path_template.match(src_path))

  def depends_on(self) -> Set[Variable]:
    variables = set(self.path_template.placeholders)
    return variables - set(self.locally_bound_variables)

  def implicitly_declared_variables(self) -> Set[Variable]:
    return set(self.path_template.placeholders) - set(self.locally_bound_variables)

  def values(self, variable: Variable, bindings: Bindings) -> Set[Value]:
    # TODO: This has obvious similarities to PathTemplate's version. What to do?
    assert variable == self.name or variable in self.path_template.placeholders
    assert variable not in self.locally_bound_variables
    assert not any(lvar in bindings for lvar in self.locally_bound_variables)
    path_template = self.path_template.with_replacements(bindings)
    if variable == self.name:
      # TODO: ugliness. Would prefer to return a set of dict[tuple->str], but dict is not hashable so set won't take it. Hrmpf.
      # TODO: important: evaluate at this point in time instead!
      return set([path_template.template])
    else:
      paths = io.glob(path_template.glob)
      values = set(self.path_template.match(path)[variable] for path in paths)
      return values

  @staticmethod
  def _is_invalid_key(key: str) -> bool:
    return not (key.startswith('{') and key.endswith('}'))


class DependentInputSpec(InputSpec):
  """An input specification whose values depend on other values. Uses lambdas"""

  def __init__(self, name: str, function: Callable) -> None:
    self.name = Variable(name)
    self.function = function
    self.inputs = [Variable(arg) for arg in getargspec(function).args]
    if name in self.inputs:
      raise InputSpecError("DependentInputSpec can not depend on variable {} which it is declaring itself.".format(name))

  def __repr__(self) -> str:
    return "Dep.In: ({})".format(self.inputs)

  def depends_on(self) -> Set[Variable]:
    return set(self.inputs)

  def matches(self, src_path: str) -> bool:
    return False

  def implicitly_declared_variables(self) -> Set[Variable]:
    return set()

  def values(self, variable: Variable, bindings: Bindings) -> List[Value]:
    assert variable == self.name
    assert all(arg in bindings for arg in self.inputs)
    arguments = [bindings[arg] for arg in self.inputs]
    values = self.function(*arguments)
    if self.name in bindings:
      bound_value = bindings[self.name]
      if bound_value in values:
        return [bound_value]
      else:
        return []
    else:
      return values

class OutputSpec(Spec):
  """Output Specification Interface"""

  @classmethod
  def build(cls, descriptor: object) -> 'OutputSpec':
    if isinstance(descriptor, str):
      path_template = PathTemplate(descriptor)
      return PathTemplateOutputSpec(path_template)
    else:
      logging.error(str(descriptor))
      raise NotImplementedError

  @abstractmethod
  def with_replacements(self, input_replacements: Mapping[Variable, str]) -> str:
    pass

  @property
  @abstractmethod
  def placeholders(self) -> List[Variable]:
    pass


class PathTemplateOutputSpec(OutputSpec):
  """Given an output spec, creates paths etc"""

  def __init__(self, path_template: PathTemplate) -> None:
    assert isinstance(path_template, PathTemplate)
    self.path_template = path_template

  def __repr__(self) -> str:
    return "Out: {}".format(self.path_template)

  @property
  def placeholders(self) -> List[Variable]:
    return self.path_template.placeholders

  def depends_on(self) -> Set[Variable]:
    return set(self.placeholders)

  def with_replacements(self, replacements: Mapping[Variable, str]) -> str:
    return self.path_template.substitute(replacements)


# Task Spec

class TaskSpec(object):
  """Data object describing which inputs a task expects."""

  variable_to_input_spec: Mapping[Variable, List[InputSpec]]

  def __init__(self, inputs: List[InputSpec], output: OutputSpec,
               src_path: str, name: str) -> None:
    self.input_specs = inputs
    self.output_spec = output
    self.src_path = src_path
    self.name = name
    self._verify_placeholders()
    self.variable_to_input_spec = collections.defaultdict(list)
    for input_spec in inputs:
      for variable in input_spec.declared_variables():
        self.variable_to_input_spec[variable].append(input_spec)


  def __repr__(self) -> str:
    ios: List[Spec] = cast(List[Spec], self.input_specs) + [self.output_spec]
    reprs = ", ".join([repr(input) for input in ios])
    return "<TaskSpec {}, ({})>".format(self.name, reprs)

  def _verify_placeholders(self) -> None:
    return
    # TODO: this currently doesn't correctly cover all cases. Disabled for now.
    # outputs = set(self.output_spec.placeholders)
    # inputs = set.union(*[input_spec.depends_on() for input_spec in self.input_specs])
    # inputs = inputs.union(set(input_spec.name for input_spec in self.input_specs))
    # if not outputs == inputs:
    #   if outputs.issuperset(inputs):
    #     difference = outputs - inputs
    #     raise ValueError("Placeholders '{}' in task_spec '{}' do not have input variables that could replace them. (Inputs: {})".format(difference, self.name, inputs))
    #   else: # TODO: this covers both subset and entirely disjoint. is the error message bringing that across? no.
    #     difference = inputs - outputs
    #     raise ValueError("Input variables '{}' in task_spec '{}' do not have any corresponding output placeholders. (Inputs: {})".format(difference, self.name, inputs))

  task_specification_glob = '/tasks/*.py'

  @classmethod
  def is_task_path(cls, path: str) -> bool:
    return fnmatch.fnmatch(path, cls.task_specification_glob)

  @property
  def input_names(self) -> List[str]:
    return [input_spec.name for input_spec in self.input_specs]

  @property
  def dependencies(self) -> Mapping[Variable, Set[Variable]]:
    return {spec.name: spec.depends_on() for spec in self.input_specs}

  def to_job_spec(self, bindings: Bindings) -> 'JobSpec':
    output_path = self.output_spec.with_replacements(bindings)
    return JobSpec(bindings, output_path, self.src_path)

  def to_job_specs(self) -> List[JobSpec]:
    return list(map(self.to_job_spec, self.all_bindings()))

  def all_bindings(self, initial_bindings: Bindings = {}) -> List[Bindings]:
    # TODO: return empty list if self.dependencies is empty???
    all_bindings = [initial_bindings]
    sorted_dependencies = toposort_flatten(self.dependencies)
    logging.debug("Sorted sorted_dependencies: %s", sorted_dependencies)
    for variable_name in sorted_dependencies:
      variable = Variable(variable_name)
      input_specs = self.variable_to_input_spec[variable]
      for input_spec in input_specs:
        logging.debug("Resolving '%s': found corresponding input spec '%s'", variable, input_spec)
        new_bindings: List[Bindings] = []
        # TODO: here, we should instead project bindings to only the variables this input_spec depends on. Then de-dupe and that should save a lot of calls. :-)
        for bindings in all_bindings:
          values = input_spec.values(variable, bindings)
          logging.debug("Got values %s for bindings %s", values, bindings)
          for value in values:
            value_binding = {variable: value}
            value_binding.update(bindings)
            new_bindings.append(value_binding)
        logging.debug("New bindings: %s", new_bindings)
        all_bindings = new_bindings
      logging.debug("Done resolving: %s", variable)
    # TODO: what if new_bindings empty because values empty?
    return all_bindings

  def matching_input_spec(self, src_path: str) -> Optional[InputSpec]:
    for input_spec in self.input_specs:
      if input_spec.matches(src_path):
        return input_spec
    return None

  def should_handle_file(self, src_path: str) -> bool:
    return self.matching_input_spec(src_path) is not None
