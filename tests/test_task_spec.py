import pytest

from flow.task_spec import *


@pytest.fixture
def simple_inputs():
  return [('glob', 'file/path/*.txt')]

@pytest.fixture
def simple_output():
  return 'some/file/path/{glob}/test.txt'

@pytest.fixture
def input_spec(simple_inputs):
  return InputSpec(simple_inputs)

@pytest.fixture
def output_spec(simple_output):
  return OutputSpec(simple_output)

@pytest.fixture
def task_spec(simple_inputs, simple_output):
  a_path = "some/file/path.py"
  return TaskSpec(simple_inputs, simple_output, a_path)


# Test InputSpec

def test_input_simple(input_spec):
  assert input_spec

def test_input_names(input_spec):
  assert input_spec.names == ['glob']


# Test OutputSpec

def test_output_simple(output_spec):
  assert output_spec

def test_output_fail_verify(output_spec):
  assert output_spec.verify_placeholders(['will-fail']) is False

def test_output_succeed_verify(output_spec):
  assert output_spec.verify_placeholders(['glob']) is True

def test_output_integrate_verify(input_spec, output_spec):
  input_names = input_spec.names
  assert output_spec.verify_placeholders(input_names) is True


# Test TaskSpec

def test_simple_task(task_spec):
  assert task_spec
  assert isinstance(task_spec.input_spec, InputSpec)
  assert isinstance(task_spec.output_spec, OutputSpec)
  assert isinstance(task_spec.path, str)
