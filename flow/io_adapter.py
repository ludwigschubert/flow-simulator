"""Adapter for local FS calls vs GC storage API calls."""

from glob import glob as os_glob
from os.path import exists as os_exists
from builtins import open as os_open

def exists(path):
  return os_exists(path)

def glob(glob_path):
  return os_glob(glob_path)

def open_file(*args, **kwargs):
  return os_open(*args, **kwargs)
