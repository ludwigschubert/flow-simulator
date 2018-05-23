import pytest
import json
from pytest import raises

from flow.task_spec import *


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

def test_path_template_format_escaped(path_template):
  replacements = {'group_id': 'subfolder/with/hierarchy', 'name_id': 'name1'}
  formatted_path = path_template.format(replacements)
  assert formatted_path == "/data/subfolder\\with\\hierarchy/names/name1.txt"

def test_path_template_capture_and_format(path_template):
  path = "/data/subfolder/names/this_is-name1.txt"
  match = path_template.match(path)
  formatted_path = path_template.format(match)
  assert formatted_path == path


# Test IterableInputSpec

def test_iis(iterable_input_spec):
  assert iterable_input_spec is not None

def test_iis_depends_on(iterable_input_spec):
  dependencies = iterable_input_spec.depends_on()
  assert not dependencies

# Test PathTemplateInputSpec

def test_ptis(path_template_input_spec):
  assert path_template_input_spec is not None

def test_ptis_depends_on(path_template_input_spec):
  dependencies = path_template_input_spec.depends_on()
  assert dependencies == {'group_id', 'name_id'}

def test_ptis_name_already_taken(path_template):
  already_taken_name = path_template.placeholders[0]
  with raises(InputSpecError):
    _ = PathTemplateInputSpec(already_taken_name, path_template)

def test_ptis_values(path_template_input_spec, mocker):
  mock_paths = [
    '/data/the_group/names/the_name1.txt',
    '/data/the_group/names/the_name2.txt',
  ]
  mocked_glob = mocker.patch('flow.task_spec.io.glob', return_value=mock_paths)

  variable = Variable('name_id')
  bindings = {'group_id': 'the_group'}
  values = path_template_input_spec.values(variable, bindings)

  mocked_glob.assert_called_once_with('/data/the_group/names/*.txt')
  assert values == {'the_name1', 'the_name2'}


# Test AggregatingInputSpec

def test_ais(aggregating_input_spec):
  assert aggregating_input_spec is not None

def test_ais_depends_on(aggregating_input_spec):
  dependencies = aggregating_input_spec.depends_on()
  assert dependencies == {'layer'}

def test_ais_wrong_dict():
  name = 'sprites'
  dictionary = {'a': 'b', 'c': 'd'}
  with raises(InputSpecError) as excinfo:
    _ = AggregatingInputSpec(name, dictionary)
  assert 'exactly one entry' in str(excinfo.value)

def test_ais_wrong_variable_declaration():
  name = 'sprites'
  dictionary = {'neuron': '/data/{layer}/{neuron}.jpg'}
  with raises(InputSpecError) as excinfo:
    _ = AggregatingInputSpec(name, dictionary)
  assert 'neuron' in str(excinfo.value)

def test_ais_unbound_variables():
  name = 'sprites'
  dictionary = {('{neuron}', '{unbound}'): '/data/{layer}/{neuron}.jpg'}
  with raises(InputSpecError) as excinfo:
    _ = AggregatingInputSpec(name, dictionary)
  assert 'unbound' in str(excinfo.value)

@pytest.fixture
def mock_paths():
  return [
    '/data/layer1/neuron1.jpg',
    '/data/layer1/neuron2.jpg',
    '/data/layer2/neuron1.jpg',
    '/data/layer2/neuron2.jpg',
  ]

def test_ais_no_bound_values(aggregating_input_spec, mocker, mock_paths):
  mocked_glob = mocker.patch('flow.task_spec.io.glob', return_value=mock_paths)

  variable = Variable('layer')
  bindings = {}
  values = aggregating_input_spec.values(variable, bindings)

  assert values == {'layer1', 'layer2'}

def test_ais_bound_values(aggregating_input_spec, mocker, mock_paths):
  mocked_glob = mocker.patch('flow.task_spec.io.glob', return_value=mock_paths)

  variable = aggregating_input_spec.name
  bindings = {'layer': 'layer1'}
  values = aggregating_input_spec.values(variable, bindings)
  assert len(values) == 1
  value = dict(list(values)[0])  # unfreezes
  golden = {'neuron': '/data/layer1/{neuron}.jpg'}
  assert value == golden

def test_ais_multiple_bound_values(aggregating_input_spec, mocker, mock_paths):
  mocked_glob = mocker.patch('flow.task_spec.io.glob', return_value=mock_paths)

  variable = aggregating_input_spec.name
  bindings = {}
  values = aggregating_input_spec.values(variable, bindings)
  assert len(values) == 1
  value = dict(list(values)[0])  # unfreezes
  golden = {'layer,neuron': '/data/{layer}/{neuron}.jpg'}
  assert value == golden

def test_ais_jobspec_unpacking(mocker, mock_paths):
  mocked_glob = mocker.patch('flow.job_spec.io.glob', return_value=mock_paths)

  input = {'layer,neuron': '/data/{layer}/{neuron}.jpg'}
  value = JobSpec.value_for_input(input)

  golden = {
   ('layer1', 'neuron1'): '/data/layer1/neuron1.jpg',
   ('layer1', 'neuron2'): '/data/layer1/neuron2.jpg',
   ('layer2', 'neuron1'): '/data/layer2/neuron1.jpg',
   ('layer2', 'neuron2'): '/data/layer2/neuron2.jpg'}
  assert value == golden

# Test DependentInputSpec

def test_dis(dependent_input_spec):
  assert dependent_input_spec is not None
  assert dependent_input_spec.inputs == ['model_name']

def test_dis_duplicate_name():
  name = 'layers'
  function = lambda layers: range(10)
  with raises(InputSpecError):
    DependentInputSpec(name, function)

def test_dis_values(dependent_input_spec):
  bindings = {'model_name': 'example'}
  variable = dependent_input_spec.name
  values = dependent_input_spec.values(variable, bindings)
  assert len(values) == 10
  assert 'example0' in values

# Test OutputSpec

def test_output_simple(output_spec):
  assert output_spec


# Test TaskSpec

def test_trivial_task_spec(trivial_task_spec):
  assert trivial_task_spec
  assert isinstance(trivial_task_spec.input_specs, List)
  assert isinstance(trivial_task_spec.input_specs[0], InputSpec)
  assert isinstance(trivial_task_spec.output_spec, OutputSpec)
  assert isinstance(trivial_task_spec.src_path, str)

@pytest.mark.skip(reason="currenlty not well defined what these conditions mean.")
def test_missing_input():
  simple_inputs = [
    PathTemplateInputSpec('pt_one', PathTemplate('/path/{one}.txt'))
  ]
  output_spec = OutputSpec.build("/some/{one}/{two}.txt")
  with raises(ValueError):
    TaskSpec(simple_inputs, output_spec, "a_path", "name")

@pytest.mark.skip(reason="currenlty not well defined what these conditions mean.")
def test_missing_output():
  simple_inputs = [
    PathTemplateInputSpec('pt_one', PathTemplate('/path/{one}.txt')),
    PathTemplateInputSpec('pt_two', PathTemplate('/different/{two}.txt'))
  ]
  output_spec = OutputSpec.build("/some/{one}/fixed.txt")
  with raises(ValueError):
    TaskSpec(simple_inputs, output_spec, "a_path", "name")

@pytest.mark.skip(reason="currenlty not well defined what these conditions mean.")
def test_mismatch_input_output():
  simple_inputs = [PathTemplateInputSpec('pt_one', PathTemplate('/path/{one}.txt'))]
  output_spec = OutputSpec.build("/some/{two}/fixed.txt")
  with raises(ValueError):
    TaskSpec(simple_inputs, output_spec, "a_path", "name")
