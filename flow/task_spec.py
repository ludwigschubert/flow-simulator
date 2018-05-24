import logging

from datetime import timedelta
from operator import itemgetter
from inspect import getargspec
from random import sample
from collections import defaultdict
from typing import NewType, List, Tuple, Any, Dict, Mapping, Optional, Union, Iterable, Callable, Set, cast, Sequence, FrozenSet
from abc import ABC, abstractmethod
from os.path import basename, splitext
import fnmatch
from functools import reduce
from toposort import toposort, toposort_flatten
from json import dumps
from numpy import mean, std
from utilspie.collectionsutils import frozendict

from flow.typing import Bindings, Variable, Value
from flow.io_adapter import io
from flow.job_spec import JobSpec
from flow.util import format_timedelta
from flow.path_template import PathTemplate, PathTemplateError


# Input & Output Spec

class Spec(ABC):
  """Abstract Superclass for InputSpec and OutputSpec"""
  # TODO: what moves here? declared variables at least?
  pass

  @abstractmethod
  def depends_on(self) -> Set[Variable]:
    """Returns the set of variables this input_spec's values depend on.
    Used to memoize calls to values(). Should not include the input_spec's
    own name, i.e. not the variable it is defining itself.
    """
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
    elif isinstance(descriptor, Sequence) and not isinstance(descriptor, str):
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
      values = set()
      for path in paths:
        match = self.path_template.match(path)
        if match:
          value = match.get(variable, None)
          if value:
            values.add(value)
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
    assert variable == self.name or variable in self.path_template.placeholders
    assert variable not in self.locally_bound_variables
    assert not any(lvar in bindings for lvar in self.locally_bound_variables)
    path_template = self.path_template.with_replacements(bindings)
    if variable == self.name:
      # TODO: important: evaluate at this point in time instead? Nope.
      aggregating = ",".join(path_template.placeholders)
      point_map = frozendict({aggregating: path_template.template})
      return set([point_map])
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

  def values(self, variable: Variable, bindings: Bindings) -> Set[Value]:
    assert variable == self.name
    assert all(arg in bindings for arg in self.inputs)
    arguments = [bindings[arg] for arg in self.inputs]
    values = self.function(*arguments)
    if self.name in bindings:
      bound_value = bindings[self.name]
      if bound_value in values:
        return set([bound_value])
      else:
        return set()
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
    return self.path_template.format(replacements)


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
    self.variable_to_input_spec = defaultdict(list)
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

  @staticmethod
  def estimate_cost(num_jobs: int, example_runs: List[JobSpec]) -> None:
    """Print naive CPU time and cost estimates based on supplied sample runs."""

    print("Estimates are 95% conf. intervals based on std of supplied runs. Only reasonable if colab instance has similar specs as requested AE instances!")
    durations = [run.execution_duration for run in example_runs]
    duration_mean, duration_std = mean(durations), std(durations)
    cpu_time_mean = timedelta(seconds=num_jobs*duration_mean)
    cpu_time_std = timedelta(seconds=num_jobs*duration_std)
    print(f"Expecting to use {format_timedelta(cpu_time_mean)}±{format_timedelta(cpu_time_std)} of CPU time.")

    price_per_hour_in_usd = 0.0526 + 2*0.0071 # 1CPU, 2GB RAM, https://cloud.google.com/appengine/pricing#flexible-environment-instances
    total_price_mean = price_per_hour_in_usd * (cpu_time_mean.total_seconds() / (60*60))
    total_price_std = price_per_hour_in_usd * (cpu_time_std.total_seconds() / (60*60))
    print(f"Expecting to cost ${total_price_mean:.2f}±{total_price_std:.2f}.")


  @property
  def input_names(self) -> List[str]:
    return [input_spec.name for input_spec in self.input_specs]

  @property
  def dependencies(self) -> Mapping[Variable, Set[Variable]]:
    return {spec.name: spec.depends_on() for spec in self.input_specs}

  def to_job_spec(self, bindings: Bindings) -> 'JobSpec':
    output_path = self.output_spec.with_replacements(bindings)
    return JobSpec(bindings, output_path, self.src_path)

  def to_job_specs(self, initial_bindings: Bindings = {}) -> Sequence[JobSpec]:
    return map(self.to_job_spec, self.all_bindings(initial_bindings))

  def all_bindings(self, initial_bindings: Bindings = {}) -> Sequence[Bindings]:
    # TODO: return empty list if self.dependencies is empty???
    all_bindings = [initial_bindings]
    sorted_dependencies = toposort_flatten(self.dependencies)
    logging.debug("Sorted sorted_dependencies: %s", sorted_dependencies)
    for variable_name in sorted_dependencies:
      variable = Variable(variable_name)
      input_specs = self.variable_to_input_spec[variable]
      for input_spec in input_specs:
        relevant_vars = input_spec.depends_on() | set([input_spec.name])
        logging.debug(f"Resolving '{variable}' via {input_spec} on relevant vars {relevant_vars}.")
        new_bindings: List[Bindings] = []
        memoized_values: Dict[FrozenSet[Tuple[Variable, Value]], Set[Value]] = {}
        for bindings in all_bindings:
          relevant_bs = frozenset((var,str(value)) for var, value in bindings.items() if var in relevant_vars)
          if relevant_bs in memoized_values:
            values = memoized_values[relevant_bs]
            logging.debug("Found cached values %s for bindings %s", list(values), bindings)
          else:
            values = input_spec.values(variable, bindings)
            # memoized_values[relevant_bs] = values
            logging.debug("Memoized values %s for bindings %s", list(values), bindings)
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

  def preflight(self, num_tried_jobs: int = 3) -> None:
    logging.info(f"Starting preflight, running {num_tried_jobs} jobs...")
    job_specs = list(self.to_job_specs())
    preflight_jobs = sample(job_specs, num_tried_jobs)
    for job in preflight_jobs:
      job.execute()
      logging.info(f"Job completed without error.")
    self.estimate_cost(len(job_specs), preflight_jobs)

  def deploy(self, preflight: bool = True) -> None:
    if preflight:
      self.preflight()
    remote_path = f"/tasks/{self.name}.py"
    io.upload(self.src_path, remote_path)
