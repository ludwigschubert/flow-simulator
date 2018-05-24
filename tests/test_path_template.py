import pytest
from pytest import raises

from flow.path_template import *

@pytest.fixture
def path_template_escaped():
  escaped = "/data/layer=escaped\\hierarchical\\name/names/{name_id}.txt"
  return PathTemplate(escaped)

# Test PathTemplate

def test_path_template_invalid_glob():
  with raises(PathTemplateError):
    actually_a_glob = "/data/*/names/*.txt"
    PathTemplate(actually_a_glob)

def test_path_template_invalid_not_canonical():
  with raises(PathTemplateError):
    not_canonical_path = "data/*/names/*.txt"
    PathTemplate(not_canonical_path)

def test_path_template_placeholders(path_template):
  assert path_template.placeholders == ["group_id", "name_id"]

def test_path_template_glob(path_template):
  assert path_template.glob == "/data/*/names/*.txt"

def test_path_template_no_match(path_template):
  path = "/data/notnames/whatever.txt"
  match = path_template.match(path)
  assert match is None

def test_path_template_capture(path_template):
  path = "/data/subfolder/names/this_is-name1.txt"
  match = path_template.match(path)
  assert match == {'group_id':'subfolder', 'name_id': 'this_is-name1'}

def test_path_template_capture_escaped(path_template):
  path = "/data/subfolder\\with\\hierarchy/names/name1.txt"
  match = path_template.match(path)
  assert match == {'group_id':'subfolder/with/hierarchy', 'name_id': 'name1'}

def test_path_template_escaped(path_template_escaped):
  path = "/data/layer=escaped\\hierarchical\\name/names/name1.txt"
  match = path_template_escaped.match(path)
  assert match == {'name_id': 'name1'}

def test_path_template_format_escaped(path_template):
  replacements = {'group_id': 'subfolder/with/hierarchy', 'name_id': 'name1'}
  formatted_path = path_template.format(replacements)
  assert formatted_path == "/data/subfolder\\with\\hierarchy/names/name1.txt"

def test_path_template_capture_and_format(path_template):
  path = "/data/subfolder/names/this_is-name1.txt"
  match = path_template.match(path)
  formatted_path = path_template.format(match)
  assert formatted_path == path
