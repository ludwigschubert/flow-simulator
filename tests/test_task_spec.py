import pytest
from pytest import raises

from flow.task_spec import *


@pytest.fixture
def simple_input():
  return ('glob', 'file/path/{name}.txt')

@pytest.fixture
def simple_inputs(simple_input):
  return [simple_input]

@pytest.fixture
def simple_output():
  return 'some/file/path/{glob}/test.txt'

@pytest.fixture
def input_spec(simple_input):
  return PathTemplateInputSpec(*simple_input)

@pytest.fixture
def output_spec(simple_output):
  return PathTemplateOutputSpec(simple_output)

@pytest.fixture
def task_spec(simple_inputs, simple_output):
  a_path = "some/file/path.py"
  name = "test_name"
  return TaskSpec(simple_inputs, simple_output, a_path, name)


# Test PathTemplate

# TODO: cleanup & DRYing
# @pytest.fixture
# def simple_path_template():
#   template = "/data/names/{name_id}.txt"
#   return PathTemplate(template)

def test_path_template_placeholders():
  path_template = PathTemplate("/data/names/{name_id}.txt")
  assert path_template.placeholders == ["name_id"]

def test_path_template_glob():
  path_template = PathTemplate("/data/names/{name_id}.txt")
  assert path_template.glob == "/data/names/*.txt"

def test_path_template_capture():
  path_template = PathTemplate("/data/names/{name_id}.txt")
  match = path_template.match("/data/names/this_is-name1.txt")
  assert match == {'name_id': 'this_is-name1'}

def test_path_template_no_match():
  path_template = PathTemplate("/data/names/{name_id}.txt")
  match = path_template.match("/data/notnames/whatever.txt")
  assert match is None

def test_path_template_glob_complex():
  path_template = PathTemplate("/data/{group_id}/names/{name_id}.txt")
  assert path_template.glob == "/data/*/names/*.txt"

def test_path_template_capture_complex():
  path_template = PathTemplate("/data/{group_id}/names/{name_id}.txt")
  path = "/data/deepviz/names/this_is-name1.txt"
  match = path_template.match(path)
  assert match == {'name_id': 'this_is-name1', 'group_id': 'deepviz'}
  assert path_template.format(match) == path

# Test InputSpec

def test_input_simple(input_spec):
  assert input_spec

def test_input_names(input_spec):
  assert input_spec.name == 'glob'


# Test OutputSpec

def test_output_simple(output_spec):
  assert output_spec


# Test TaskSpec

def test_simple_task(task_spec):
  assert task_spec
  assert isinstance(task_spec.input_specs, List)
  assert isinstance(task_spec.input_specs[0], InputSpec)
  assert isinstance(task_spec.output_spec, OutputSpec)
  assert isinstance(task_spec.src_path, str)

def test_missing_input():
  simple_inputs = [('one', 'path/{one}.txt')]
  simple_output = "some/{one}/{two}.txt"
  with raises(ValueError):
    TaskSpec(simple_inputs, simple_output, "a_path", "name")

def test_missing_output():
  simple_inputs = [('one', 'path/{one}.txt'), ('two', 'different/{two}.txt')]
  simple_output = "some/{one}/fixed.txt"
  with raises(ValueError):
    TaskSpec(simple_inputs, simple_output, "a_path", "name")

def test_mismatch_input_output():
  simple_inputs = [('one', 'path/{one}.txt')]
  simple_output = "some/{two}/fixed.txt"
  with raises(ValueError):
    TaskSpec(simple_inputs, simple_output, "a_path", "name")
