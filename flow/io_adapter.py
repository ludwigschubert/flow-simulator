"""Adapter for local FS calls vs GC storage API calls."""
from typing import Any, List, Set, TextIO, Tuple, Optional, BinaryIO
from contextlib import contextmanager, closing
import logging
from abc import ABC, abstractmethod
import fnmatch
from os.path import join, dirname
from os import makedirs

from flow.util import memoize_single_arg


# TODO: reify concept of flow-paths as a type?

class IOAdapter(ABC):

  @contextmanager
  def reading(self, path: str) -> BinaryIO:
    normalized = self.normpath(path)
    with self._reading(normalized) as reading_file:
      yield reading_file

  @contextmanager
  def writing(self, path: str) -> BinaryIO:
    normalized = self.normpath(path)
    self._makedirs(normalized)
    with self._writing(normalized) as writing_file:
      yield writing_file

  def glob(self, path: str) -> List[str]:
    normalized = self.normpath(path)
    return self._glob(normalized)

  def exists(self, path: str) -> bool:
    normalized = self.normpath(path)
    values = self._exist([normalized])
    assert len(values) == 1
    return values[0]

  def exist(self, paths: List[str]) -> List[bool]:
    normalized = [self.normpath(path) for path in paths]
    return self._exist(normalized)

  def download(self, path: str) -> str:
    normalized = self.normpath(path)
    return self._download(normalized)

  def upload(self, local_path: str, remote_path: str) -> None:
    normalized_remote_path = self.normpath(remote_path)
    return self._upload(local_path, normalized_remote_path)

  @abstractmethod
  def normpath(self, path: str) -> str:
    """Transforms a canonical path to a form compatible with the IOAdapter.

    For example, we may convert "absolute" flow-style paths like '/some/file.py'
    to a local, relative path like 'playground/some/file.py'.
    """
    pass

  @abstractmethod
  @contextmanager
  def _reading(self, path: str) -> BinaryIO:
    pass

  @abstractmethod
  @contextmanager
  def _writing(self, path: str) -> BinaryIO:
    pass

  @abstractmethod
  def _makedirs(self, path: str) -> None:
    pass

  @abstractmethod
  def _glob(self, path: str) -> List[str]:
    pass

  @abstractmethod
  def _exist(self, paths: List[str]) -> List[bool]:
    pass

  @abstractmethod
  def _download(self, path: str) -> str:
    pass

  @abstractmethod
  def _upload(self, local_path: str, remote_path: str) -> None:
    pass


# LocalFSAdapter

from glob import glob as localfs_glob
from builtins import open as localfs_open
from os.path import exists as localfs_exists
from os.path import normpath as localfs_normpath
from os.path import dirname as localfs_dirname
from os import makedirs as localfs_makedirs

class LocalFSAdapter(IOAdapter):

  def normpath(self, path: str) -> str:
    path = localfs_normpath(path)
    return path

  @contextmanager
  def _reading(self, path: str) -> BinaryIO:
    reading_file = localfs_open(path, mode='r+b')
    yield reading_file
    reading_file.close()

  @contextmanager
  def _writing(self, path: str) -> BinaryIO:
    writing_file = localfs_open(path, mode='w+b')
    yield writing_file
    writing_file.close()

  def _makedirs(self, path: str) -> None:
    dirpath = localfs_dirname(path)
    localfs_makedirs(dirpath, exist_ok=True)

  def _glob(self, glob_path: str) -> List[str]:
    logging.debug(glob_path)
    paths = localfs_glob(glob_path)
    logging.debug(str(paths))
    # we need to remove the root dir from all returned paths!

    return paths#[path[prefix_length:] for path in paths]

  def _exist(self, paths: List[str]) -> List[bool]:
    return [localfs_exists(path) for path in paths]

  def _download(self, path: str) -> str:
    """No-op on local fs. Returns LOCAL path!"""
    return path

  def _upload(self, local_path: str, remote_path: str) -> None:
    raise NotImplementedError("local fs has no concept of uploading")


# GCStorageAdapter

from google.cloud import storage
from tempfile import SpooledTemporaryFile, mkdtemp

class GCStorageAdapter(IOAdapter):

  def __init__(self, project: str = 'brain-deepviz',
                     bucket: str = 'lucid-flow') -> None:
    self.client = storage.Client(project=project)
    self.bucket = self.client.bucket(bucket)
    self.tempdir = mkdtemp()

  def normpath(self, path: str) -> str:
    # logging.debug(f"normpathing: {path}")
    if path.startswith('gs://'):
      path = path[5:]
    # logging.debug(f"removed gs scheme: {path}")
    if path.startswith(self.bucket.name):
      path = path[len(self.bucket.name):]
    # logging.debug(f"removed bucket: {path}")
    if path.startswith('/'):
      path = path[1:]
    # logging.debug(f"removed leading slash: {path}")
    return path

  @contextmanager
  def _reading(self, path: str) -> BinaryIO:
    local_path = self._download(path)
    reading_file = localfs_open(local_path, mode='r+b')
    yield reading_file
    reading_file.close()

  @contextmanager
  def _writing(self, path: str) -> BinaryIO:
    blob = storage.blob.Blob(path, self.bucket)
    local_path = join(self.tempdir, path)
    makedirs(dirname(local_path), exist_ok=True)
    writing_file = localfs_open(local_path, mode='w+b')
    yield writing_file
    writing_file.close()
    blob.upload_from_filename(local_path)

  def _makedirs(self, path: str) -> None:
    pass

  @memoize_single_arg
  def _glob(self, glob_path: str) -> List[str]:
    fields = 'items/name,items/updated,nextPageToken'
    matched_paths: List[str] = []
    # GCS returns folders iff a trailing slash is specified, so we try both:
    for folder_suffix in ['', '/']:
      glob_string = glob_path + folder_suffix
      prefix = glob_string.split('*')[0] # == entire string if no '*' found
      bucket_listing = self.bucket.list_blobs(fields=fields, prefix=prefix)
      file_paths = [blob.name for blob in bucket_listing]
      matched_paths += fnmatch.filter(file_paths, glob_string)
    # matched_paths = list(sorted(set(matched_paths)))  # should already be unique
    return [f"gs://{self.bucket.name}/{path}" for path in matched_paths]

  def _exist(self, paths: List[str]) -> List[bool]:
    with self.client.batch():
      bools = [storage.blob.Blob(path, self.bucket).exists() for path in paths]
    return bools

  def _download(self, path: str) -> str:
    local_path = join(self.tempdir, path)
    makedirs(dirname(local_path), exist_ok=True)
    blob = storage.blob.Blob(path, self.bucket)
    blob.download_to_filename(local_path)
    return local_path

  def _upload(self, local_path: str, remote_path: str) -> None:
    blob = storage.blob.Blob(remote_path, self.bucket)
    blob.upload_from_filename(local_path)

from os import getenv
io: IOAdapter
if getenv('USE_LOCAL_FS', '').startswith('TRUE'):
  logging.warn("Using LocalFSAdapter!")
  io = LocalFSAdapter()
else:
  io = GCStorageAdapter()
