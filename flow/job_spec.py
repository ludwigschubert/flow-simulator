import logging
from typing import List, Tuple, Any, Dict
import json as JSON
from imp import load_source
from os.path import basename, splitext, join, exists
from os import getenv
from timeit import default_timer as timer

from flow.typing import Bindings, Variable, Value
from flow.io_adapter import io
from flow.dynamic_import import import_module_from_local_source
from flow.path_template import PathTemplate, PathTemplateError
from flow.path import AbsolutePath, AbsoluteGCSURL

from lucid.misc.io import load, save


class JobSpec(object):
    """Serializable data object describing which task to execute and its bindings."""

    def __init__(
        self, bindings: Bindings, output: str, task_path: AbsolutePath
    ) -> None:
        self.bindings = bindings
        self.output = output
        self.task_path = task_path

    def __eq__(self, other: object) -> bool:
        if isinstance(self, other.__class__):
            return self.__dict__ == other.__dict__
        return False

    def __repr__(self) -> str:
        return "<JobSpec {self.task_path}({self.bindings}) -> {self.output}>".format(
            self=self
        )

    @classmethod
    def value_for_input(cls, input: object) -> object:
        if isinstance(input, str):
            # logging.debug("input is str")
            # TODO: here is where we would add convenience loading etc.
            if input.startswith("/"):
                return AbsoluteGCSURL.from_absolute_path(AbsolutePath(input))
            else:
                return input
        if isinstance(input, dict):
            # logging.debug("input is dict!")
            # this signifies an aggregating input spec! let's resolve it:
            if len(input.items()) == 1:
                logging.debug("input dict had one entry, assuming AggregatingIS")
                placeholder_string, path_template_string = list(input.items())[0]
                path_template = PathTemplate(path_template_string)
                placeholders = placeholder_string.split(",")
                assert placeholders == path_template.placeholders
                paths = io.glob(path_template.glob)
                value = {}
                for path in paths:
                    matches = path_template.match(path)
                    if matches:
                        key = tuple(matches[ph] for ph in placeholders)
                        value[key] = AbsoluteGCSURL.from_absolute_path(
                            AbsolutePath(path)
                        )
                return value
            else:
                logging.debug("input dict had multiple entries, simply setting value.")
                return input
        if isinstance(input, (int, float, tuple, list, dict, set)):
            # logging.debug("input is value")
            return input
        else:
            raise NotImplementedError

    @classmethod
    def value_for_output(cls, output: str) -> str:
        if isinstance(output, str):
            # TODO: decide if we can't find a better way to represent paths
            # E.g. maybe we should just not allow tasks to save their own results.
            # or provide a write_handle or sth.
            if output.startswith("/"):
                return AbsoluteGCSURL.from_absolute_path(AbsolutePath(output))
            else:
                return output
        else:
            raise NotImplementedError

    @classmethod
    def save_result_for_output(cls, result: object, output: object) -> None:
        """TODO: move io_adapter into lucid.misc.io and DELETE this!"""
        if result is None:
            logging.info(
                f"Task did not return a result. You can check the declared output: {output}"
            )
            return
        if isinstance(output, str):
            # if output.startswith("/"): # = is a canonical path
            if isinstance(result, str):
                with io.writing(output) as output_file:
                    output_file.write(result.encode())
                # TODO: loaders and savers? Assume serialized already for now.
            else:
                with io.writing(output) as output_file:
                    save(result, output_file)
        else:
            raise NotImplementedError

    def execute(self) -> Any:
        start = timer()
        # load module
        task_path = self.task_path
        if not exists(task_path):
            task_path = io.download(task_path)
        module = import_module_from_local_source(task_path)
        # set bindings
        for name, input in self.bindings.items():
            value = self.value_for_input(input)
            logging.debug("Setting '%s' to '%s' in module '%s'", name, value, module)
            setattr(module, name, value)
        # set output; e.g. in case the task saves its own results
        output_value = self.value_for_output(self.output)
        logging.debug("Setting 'output' to '%s' in module '%s'", output_value, module)
        setattr(module, "output", output_value)
        # execute and save result
        self.result = module.main()  # type: ignore
        end = timer()
        self.execution_duration = end - start
        self.save_result_for_output(self.result, self.output)
        # unload module
        # TODO: test if that actually allows us to call this method multiple times!
        del module
        return self.result

    # Serialization

    @classmethod
    def from_json(cls, json: str) -> "JobSpec":
        dict = JSON.loads(json)
        return JobSpec(**dict)

    def to_json(self, pretty: bool = False) -> str:
        if pretty:
            return JSON.dumps(self.__dict__, indent=2, sort_keys=True)
        else:
            return JSON.dumps(self.__dict__)
