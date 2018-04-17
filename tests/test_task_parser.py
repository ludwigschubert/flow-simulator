from pytest import raises
import logging
from flow.task_parser import TaskParser, TaskParseError

def test_parser_init_empty():
  with raises(TypeError):
    _ = TaskParser()

def test_parser_init_none():
  with raises(TaskParseError):
    _ = TaskParser(None)

def test_parser_init_empty_task():
  with raises(TaskParseError):
    parser = TaskParser("tests/fixtures/task_specs/empty.py")

def test_parser_init_no_output_task():
  with raises(TaskParseError):
    parser = TaskParser("tests/fixtures/task_specs/no_output.py")

def test_parser_init_no_main_task():
  with raises(TaskParseError):
    parser = TaskParser("tests/fixtures/task_specs/no_main.py")

def test_parser_init_no_inputs_task():
  with raises(TaskParseError):
    parser = TaskParser("tests/fixtures/task_specs/no_inputs.py")

def test_parser_init_simple_task():
  parser = TaskParser("tests/fixtures/task_specs/simple.py")

def test_parser_to_task_spec():
  parser = TaskParser("tests/fixtures/task_specs/simple_with_list.py")
  spec = parser.to_spec()
  assert spec is not None
