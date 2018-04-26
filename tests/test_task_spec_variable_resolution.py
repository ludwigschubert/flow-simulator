import pytest
from flow.task_spec import *

from toposort import toposort, toposort_flatten

@pytest.fixture
def iis1():
  return IterableInputSpec('iis1', [0,1])

@pytest.fixture
def iis2():
  return IterableInputSpec('iis2', ['a', 'b'])

@pytest.fixture
def iisos():
  return OutputSpecFactory.build('/{iis1}/{iis2}.txt')

@pytest.fixture
def fully_orthogonal(iis1, iis2, iisos):
  return TaskSpec([iis1, iis2], iisos, '', '')


def test_fully_orthogonal(fully_orthogonal):
  i1, i2 = fully_orthogonal.input_names
  all_bindings = fully_orthogonal.all_bindings()
  assert all(binding in all_bindings for binding in [
    {i1: 0, i2: 'a'},
    {i1: 0, i2: 'b'},
    {i1: 1, i2: 'a'},
    {i1: 1, i2: 'b'}
  ])

def test_fully_orthogonal_intersecting_values(iis1, iis2, iisos):
  intersects_with_iis1 = IterableInputSpec('iis1', [1, 2])
  task_spec = TaskSpec([iis1, iis2, intersects_with_iis1], iisos, '', '')
  all_bindings = task_spec.all_bindings()
  assert all(binding in all_bindings for binding in [
    {'iis1': 1, 'iis2': 'a'},
    {'iis1': 1, 'iis2': 'b'}
  ])

def test_dependent_but_orthogonal(iis1, iisos):
  dependent = DependentInputSpec('iis2', lambda iis1: ['a', 'b'] )
  task_spec = TaskSpec([iis1, dependent], iisos, '', '')
  all_bindings = task_spec.all_bindings()
  assert all(binding in all_bindings for binding in [
    {'iis1': 0, 'iis2': 'a'},
    {'iis1': 0, 'iis2': 'b'},
    {'iis1': 1, 'iis2': 'a'},
    {'iis1': 1, 'iis2': 'b'}
  ])
  job_specs = task_spec.to_job_specs()
  assert len(job_specs) == 4

def test_dependent(iis1, iisos):
  dependent = DependentInputSpec('iis2', lambda iis1: str(iis1) )
  task_spec = TaskSpec([iis1, dependent], iisos, '', '')
  all_bindings = task_spec.all_bindings()
  assert all(binding in all_bindings for binding in [
    {'iis1': 0, 'iis2': '0'},
    {'iis1': 1, 'iis2': '1'},
  ])
