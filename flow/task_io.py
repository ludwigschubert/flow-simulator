from typing import Sequence
import logging

from flow.io_adapter import io
from lucid.misc.io import load as lucid_io_load

def load(path: str, transform: str = None) -> Sequence:
  logging.warn(path)
  with io.reading(path) as handle:
    result = lucid_io_load(handle)
  if transform == 'lines':
    result = result.split("\n")
  return result
