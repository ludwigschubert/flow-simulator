"""
These tests are slow and brittle!
They talk to the internet and assume state.
"""

import logging
import pytest

from flow.io_adapter import GCStorageAdapter

@pytest.fixture
def gcs():
  return GCStorageAdapter()

def test_normpath(gcs):
  result = gcs.normpath('/tasks/*/test.py')
  assert result == 'tasks/*/test.py'

def test_glob(gcs):
  results = gcs.glob('/tasks/*.py')
  assert '/tasks/' not in results
  assert '/tasks/greetings.py' in results
  assert '/tasks/not-a-task' not in results

def test_exists(gcs):
  exists = gcs.exists('/tasks/greetings.py')
  assert exists

def test_reading(gcs):
  with gcs.reading('/data/names/name1.txt') as tmpfile:
    content = tmpfile.read().decode()
  assert content == "Katherine"
