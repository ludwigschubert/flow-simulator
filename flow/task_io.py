from typing import Sequence
import logging

from flow.io_adapter import io
from flow.path_template import PathTemplate
from flow.path import AbsolutePath
from lucid.misc.io import load as lucid_io_load


from absl import flags

FLAGS = flags.FLAGS


def load(path: str, transform: str = "None") -> Sequence:
    assert path.startswith("/")
    # path = PathTemplate.path_template_prefix + raw_path # TODO: rethink
    with io.reading(AbsolutePath(path)) as handle:
        result = lucid_io_load(handle)
    if transform == "lines":
        result = result.split("\n")
    return result
