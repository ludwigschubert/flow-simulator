import logging
import pytest
import os
from pytest import raises
from unittest.mock import patch

from flow.typing import Bindings, Variable, Value
from flow.job_spec import JobSpec
import flow
from io import StringIO

simple_job = {
  'bindings': [('name', 'Ludwig'), ('x', 2)],
  'output': 'tests/fixtures/data/salutations/Ludwig-2.txt',
  'task_path': 'tests/fixtures/task_specs/simple.py'
}

@pytest.fixture
def simple_job_spec():
  return JobSpec(**simple_job)

def test_init_empty():
  with raises(TypeError):
    _ = JobSpec()

def test_init_simple(simple_job_spec):
  assert simple_job_spec

def test_serialization_simple(simple_job_spec):
  json = simple_job_spec.to_json()
  assert json is not None
  assert 'inputs' in json
  assert 'output' in json
  assert 'path' in json
  assert 'name' in json

def test_deserialization_simple():
  json = open('tests/fixtures/job_specs/simple.json').read()
  new_spec = JobSpec.from_json(json)
  assert new_spec is not None
  # check that arrays get converted back to tuples:
  assert new_spec.inputs == {"name": "Ludwig"}

def test_execute(simple_job_spec, mocker):
  file_stub = mocker.MagicMock()
  mocked_open = mocker.patch('flow.job_spec.io.writing', return_value=file_stub)

  simple_job_spec.execute()

  mocked_open.assert_called_once_with(simple_job_spec.output)
  file_stub.__enter__().write.assert_called_once_with(simple_job_spec.result.encode())
