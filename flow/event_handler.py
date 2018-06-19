import logging

from enum import Enum
from typing import Optional, List
from itertools import product
from collections import ChainMap
import json

from flow.io_adapter import io
from flow.task_parser import TaskParser, TaskParseError
from flow.task_spec import TaskSpec, PathTemplateOutputSpec
from flow.job_spec import JobSpec
from flow.queue import get_enqueuer


class FileEventHandler(object):
    """Provides `handle_file_event` which takes care of new files."""

    _task_specs: List[TaskSpec]
    # TODO: make this a flag?
    def __init__(self) -> None:
        self._task_specs = []
        self.enqueuer = get_enqueuer()

    # TODO: rethionk caching here
    @property
    def task_specs(self) -> List[TaskSpec]:
        if self._task_specs and len(self._task_specs) > 0:
            return self._task_specs
        paths = io.file_list.glob(TaskSpec.task_specification_glob)
        specs = [TaskParser(path).to_spec() for path in paths]
        self._write_manifest_if_needed(specs)
        self._task_specs = specs
        logging.info("EventHandler loaded known task specs: %s", specs)
        return self._task_specs

    def handle_file_event(self, src_path: str) -> None:
        if TaskSpec.is_task_path(src_path):
            self._handle_new_task(src_path)
        else:
            self._handle_new_input(src_path)

    def _handle_new_task(self, src_path: str) -> None:
        logging.info("Handling new task: %s", src_path)
        try:
            task_spec = TaskParser(src_path).to_spec()
            self._create_jobs(task_spec)  # no src_path!
            # TODO: rethink caching here
            self._task_specs = []
        except TaskParseError as e:
            logging.error("Parsing task at '%s' failed! Message: %s", src_path, e)

    def _handle_new_input(self, src_path: str) -> None:
        logging.info("Handling new input: %s", src_path)

        relevant = [
            task_spec
            for task_spec in self.task_specs
            if task_spec.should_handle_file(src_path)
        ]
        if relevant:
            logging.info(
                "Found %d relevant tasks: %s",
                len(relevant),
                ", ".join([ts.name for ts in relevant]),
            )
            for task_spec in relevant:
                self._create_jobs(task_spec, src_path)
        else:
            logging.info("No relevant tasks found for file %s", src_path)

    def _create_jobs(self, task_spec: TaskSpec, src_path: Optional[str] = None) -> None:
        """Adds all new jobs for this task.

    If no `src_path` is supplied, assumes the task itself is new and adds all
    possible jobs for it.
    """
        logging.info("Creating new jobs for task '%s'.", task_spec.name)

        # if src_path:
        # matched_input_spec = task_spec.matching_input_spec(src_path)
        # else:
        #   matched_input_spec = None
        #
        # inputs = []
        # for input_spec in task_spec.input_specs:
        #   if input_spec == matched_input_spec:
        #     values = matched_input_spec.values(src_path)
        #     logging.info("Fixing input for input_spec '%s' to %s", matched_input_spec, values)
        #   else:
        #     values = input_spec.values()
        #     logging.info("Found %d inputs for input_spec '%s'", len(values), input_spec.name)
        #   inputs.append(values)
        #
        # job_specs = []
        # for args_and_assignments in product(*inputs):
        #   args, assignments = zip(*args_and_assignments) # unzip
        #   assert len(args) == len(task_spec.input_names)
        #   job_inputs = list(zip(task_spec.input_names, args))
        #   replacements = ChainMap(*assignments) # merges dicts
        #   job_output = task_spec.output_spec.output_path(replacements)
        #   job_spec = JobSpec(job_inputs, job_output, task_spec.src_path)
        #   job_specs.append(job_spec)
        # TODO: fix input dimension again!
        job_specs = list(task_spec.to_job_specs())
        logging.info("Created {} job_specs, enqueueing...".format(len(job_specs)))
        self.enqueuer.add(job_specs)

    def _write_manifest_if_needed(self, task_specs: List[TaskSpec]) -> None:
        for task_spec in task_specs:
            if not io.exists(task_spec.manifest_path):
                bindings = task_spec.all_bindings()
                keys = task_spec.output_spec.placeholders
                assignments = sorted(
                    [binding[key] for key in keys] for binding in bindings
                )
                if isinstance(task_spec.output_spec, PathTemplateOutputSpec):
                    object = {
                        "output": {
                            "template": task_spec.output_spec.path_template.template
                        },
                        "bindings": {"values": assignments},
                    }
                    with io.writing(task_spec.manifest_path, mode="w") as open_file:
                        json.dump(object, open_file)
                else:
                    raise NotImplementedError


class JobEventHandler(object):
    """Provides `handle_job_event` which takes care of new JobSpecs coming in as JSON."""

    def __init__(self) -> None:
        pass

    def handle_job_event(self, serialized_job_spec: str) -> None:
        job_spec = JobSpec.from_json(serialized_job_spec)
        job_spec.execute()
