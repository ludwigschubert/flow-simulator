import pytest


# Paths
from flow.path import RelativePath, AbsolutePath, AbsoluteURL


@pytest.fixture
def relative_path():
    return RelativePath("a/relative/path.ext")


@pytest.fixture
def absolute_path():
    return AbsolutePath("/an/absolute/path.ext")


@pytest.fixture
def absolute_url():
    return AbsoluteURL("gs://bucket/an/absolute/path.ext")


# PathTemplate
from flow.task_spec import PathTemplate

PathTemplate.path_template_prefix = ""


@pytest.fixture
def path_template():
    return PathTemplate("/data/{group_id}/names/{name_id}.txt")


# InputSpec
from flow.task_spec import (
    IterableInputSpec,
    PathTemplateInputSpec,
    AggregatingInputSpec,
    DependentInputSpec,
)


@pytest.fixture
def iterable_input_spec():
    return IterableInputSpec("iterable_input_spec", range(10))


@pytest.fixture
def path_template_input_spec(path_template):
    return PathTemplateInputSpec("path_template_input_spec", path_template)


@pytest.fixture
def aggregating_input_spec():
    name = "aggregating_input_spec"
    dictionary = {"{neuron}": "/data/{layer}/{neuron}.jpg"}
    return AggregatingInputSpec(name, dictionary)


@pytest.fixture
def dependent_input_spec():
    name = "dependent_input_spec"
    function = lambda model_name: [model_name + str(i) for i in range(10)]
    return DependentInputSpec(name, function)


# OutputSpec
from flow.task_spec import OutputSpec, PathTemplateOutputSpec


@pytest.fixture
def simple_output():
    return "/some/file/path/{glob}/test.txt"


@pytest.fixture
def output_spec(simple_output):
    return OutputSpec.build(simple_output)


# JobSpec
from flow.job_spec import JobSpec


@pytest.fixture
def noop_job_spec():
    return JobSpec({"unity": 1}, "/data/noop", "/tasks/noop.py")


# TaskSpec

from flow.task_spec import TaskSpec


@pytest.fixture
def trivial_task_spec():
    iis1 = IterableInputSpec("iis1", [0, 1])
    iis2 = IterableInputSpec("iis2", ["a", "b"])
    output_spec = OutputSpec.build("/{iis1}/{iis2}.txt")
    a_path = "/tasks/trivial_task_spec.py"
    name = "trivial_task_spec.py"
    return TaskSpec([iis1, iis2], output_spec, a_path, name)


@pytest.fixture
def task_spec_repetetive():
    iis1 = IterableInputSpec("iis1", [0, 1])
    iis2 = IterableInputSpec("iis2", ["a", "b"])
    output_spec = OutputSpec.build("/{iis1}/{iis2}.txt")
    a_path = "/tasks/trivial_task_spec.py"
    name = "trivial_task_spec.py"
    return TaskSpec([iis1, iis2], output_spec, a_path, name)


@pytest.fixture
def full_task_spec(
    iterable_input_spec, aggregating_input_spec, dependent_input_spec, output_spec
):
    path_template = PathTemplate(
        "/data/models/{model_name}/checkpoints/{checkpoint_folder}"
    )
    path_template_input_spec = PathTemplateInputSpec("checkpoint_path", path_template)
    inputs = [
        iterable_input_spec,
        path_template_input_spec,
        aggregating_input_spec,
        dependent_input_spec,
    ]
    output = output_spec
    a_path = "/tasks/full_task_spec.py"
    name = "full_task_spec.py"
    return TaskSpec(inputs, output, a_path, name)
