import logging
import pytest

from flow.io_adapter import LocalFSAdapter

pytestmark = pytest.mark.skip

@pytest.fixture
def lfs():
  return LocalFSAdapter(root_dir="tests/fixtures")

def test_normpath(lfs):
  result = lfs.normpath('/tasks/*/test.py')
  assert result == 'tests/fixtures/tasks/*/test.py'

def test_glob(lfs):
  results = lfs.glob('/data/names/*.txt')
  assert '/data/names/' not in results
  assert '/data/names/name1.txt' in results

def test_exists(lfs):
  exists = lfs.exists('/data/names/name1.txt')
  assert exists

def test_reading(lfs):
  with lfs.reading('/data/names/name1.txt') as tmpfile:
    content = tmpfile.read().decode()
  assert content == "Katherine"
