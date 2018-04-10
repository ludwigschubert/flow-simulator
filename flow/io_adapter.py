"""Adapter for local FS calls vs GC storage API calls."""
from typing import Any, List, TextIO, Tuple, Optional

from glob import glob as os_glob
from os.path import exists as os_exists
from builtins import open as os_open


def exists(path: str) -> bool:
  return os_exists(path)


def glob(glob_path: str) -> List[str]:
  return os_glob(glob_path)


def open_file(path: str) -> TextIO:
  return os_open(path, mode='w')
