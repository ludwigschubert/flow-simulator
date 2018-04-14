"""Adapter for local FS calls vs GC storage API calls."""
from typing import Any, List, TextIO, Tuple, Optional, BinaryIO
from contextlib import contextmanager, closing
import logging
from abc import ABC, abstractmethod
import fnmatch
from os.path import join


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
    with self._writing(normalized) as writing_file:
      yield writing_file

  def glob(self, path: str) -> List[str]:
    normalized = self.normpath(path)
    return self._glob(normalized)

  def exists(self, path: str) -> bool:
    normalized = self.normpath(path)
    return self._exists(normalized)

  def download(self, path: str) -> str:
    normalized = self.normpath(path)
    return self._download(normalized)

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
  def _glob(self, path: str) -> List[str]:
    pass

  @abstractmethod
  def _exists(self, path: str) -> bool:
    pass

  @abstractmethod
  def _download(self, path: str) -> str:
    pass


# LocalFSAdapter

from glob import glob as localfs_glob
from builtins import open as localfs_read
from os.path import exists as localfs_exists
from os.path import normpath as localfs_normpath

class LocalFSAdapter(IOAdapter):

  def __init__(self, root_dir: str) -> None:
    self.root_dir = root_dir

  def __repr__(self) -> str:
    return "LocalFSAdapter (root: {})".format(self.root_dir)

  def normpath(self, path: str) -> str:
    path = localfs_normpath(path)
    if path.startswith('/'):
      path = path[1:]
      path = join(self.root_dir, path)
    return path

  @contextmanager
  def _reading(self, path: str) -> BinaryIO:
    reading_file = localfs_read(path, mode='r+b')
    yield reading_file
    reading_file.close()

  @contextmanager
  def _writing(self, path: str) -> BinaryIO:
    writing_file = localfs_read(path, mode='w+b')
    yield writing_file
    writing_file.close()

  def _glob(self, glob_path: str) -> List[str]:
    logging.debug(glob_path)
    paths = localfs_glob(glob_path)
    logging.debug(str(paths))
    # we need to remove the root dir from all returned paths!
    prefix_length = len(self.root_dir)
    return [path[prefix_length:] for path in paths]

  def _exists(self, path: str) -> bool:
    return localfs_exists(path)

  def _download(self, path: str) -> str:
    """No-op on local fs. Returns LOCAL path!"""
    return path


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
    if path.startswith('/'):
      path = path[1:]
    return path

  @contextmanager
  def _reading(self, path: str) -> BinaryIO:
    blob = storage.blob.Blob(path, self.bucket)
    tmpfile = SpooledTemporaryFile() # writes to disk only if necessary
    blob.download_to_file(tmpfile)
    tmpfile.seek(0)
    yield tmpfile
    tmpfile.close()

  @contextmanager
  def _writing(self, path: str) -> BinaryIO:
    blob = storage.blob.Blob(path, self.bucket)
    tmpfile = SpooledTemporaryFile() # writes to disk only if necessary
    yield tmpfile
    tmpfile.seek(0)
    blob.upload_from_file(tmpfile)

  def _glob(self, glob_path: str) -> List[str]:
    prefix = glob_path.split('*')[0] # == entire string if no '*' found
    fields = 'items/name,items/updated,nextPageToken'
    bucket_listing = self.bucket.list_blobs(fields=fields, prefix=prefix)
    file_paths = [blob.name for blob in bucket_listing]
    matched_paths = fnmatch.filter(file_paths, glob_path)
    return ['/' + path for path in matched_paths]

  def _exists(self, path: str) -> bool:
    blob = storage.blob.Blob(path, self.bucket)
    return bool(blob)

  def _download(self, path: str) -> str:
    local_path = join(self.tempdir, path)
    blob = storage.blob.Blob(path, self.bucket)
    blob.download_to_filename(local_path)
    return local_path


from os import getenv
io: IOAdapter
if getenv('USE_LOCAL_FS', '').startswith('TRUE'):
  logging.warn("Using LocalFSAdapter!")
  io = LocalFSAdapter(root_dir='.')
else:
  io = GCStorageAdapter()
