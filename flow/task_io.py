from typing import Sequence
import logging

from flow.io_adapter import io
from lucid.misc.io import load as lucid_io_load

from absl import flags
FLAGS = flags.FLAGS

def load(raw_path: str, transform: str = None) -> Sequence:
  assert raw_path.startswith('/')
  path = FLAGS.path_template_prefix + raw_path
  with io.reading(path) as handle:
    result = lucid_io_load(handle)
  if transform == 'lines':
    result = result.split("\n")
  return result