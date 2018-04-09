from pytest import raises
from flow.task_parser import TaskParser

def test_parser_init_empty():
  with raises(TypeError):
    _ = TaskParser()

def test_parser_init_none():
  with raises(ValueError):
    _ = TaskParser(None)

def test_parser_init_invalid_path():
  with raises(ValueError):
    parser = TaskParser("invalid/file/path.py")

def test_parser_init_empty_task():
  with raises(ValueError):
    parser = TaskParser("tests/fixtures/task_specs/empty.py")

def test_parser_init_no_output_task():
  with raises(ValueError):
    parser = TaskParser("tests/fixtures/task_specs/no_output.py")

def test_parser_init_no_main_task():
  with raises(ValueError):
    parser = TaskParser("tests/fixtures/task_specs/no_main.py")

def test_parser_init_no_inputs_task():
  with raises(ValueError):
    parser = TaskParser("tests/fixtures/task_specs/no_inputs.py")

def test_parser_init_simple_task():
  parser = TaskParser("tests/fixtures/task_specs/simple.py")
