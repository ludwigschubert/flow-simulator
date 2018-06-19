"""Adapter for local FS calls vs GC storage API calls."""
from typing import Any, List, Set, TextIO, Tuple, Optional, IO
from contextlib import contextmanager, closing
import logging
from abc import ABC, abstractmethod
import fnmatch
from os.path import join, dirname
from os import makedirs

from flow.util import memoize, batch
from flow.file_list import FileList
from flow.path import AbsolutePath, RelativePath


class IOAdapter(ABC):

    file_list: FileList

    @contextmanager
    def reading(self, path: AbsolutePath) -> IO:
        normalized = self.normpath(path)
        with self._reading(normalized) as reading_file:
            yield reading_file

    @contextmanager
    def writing(self, path: str, mode: str = "w+b") -> IO:
        normalized = self.normpath(path)
        self._makedirs(normalized)
        with self._writing(normalized, mode=mode) as writing_file:
            yield writing_file

    def glob(self, path: str) -> List[AbsolutePath]:
        normalized = self.normpath(path)
        return self._glob(normalized)

    def exists(self, path: AbsolutePath) -> bool:
        normalized = self.normpath(path)
        values = self._exist([normalized])
        assert len(values) == 1
        return values[0]

    def exist(self, paths: List[AbsolutePath]) -> List[bool]:
        normalized = [AbsolutePath(self.normpath(path)) for path in paths]
        return self._exist(normalized)

    def download(self, path: str) -> AbsolutePath:
        normalized = self.normpath(path)
        local_path = self._download(normalized)
        logging.debug("Downloaded `%s` to `%s`.", path, local_path)
        return local_path

    def upload(self, local_path: str, remote_path: str) -> None:
        normalized_remote_path = self.normpath(remote_path)
        relative_remote_path = normalized_remote_path.as_relative_path()
        return self._upload(local_path, relative_remote_path)

    @abstractmethod
    def normpath(self, path: str) -> AbsolutePath:
        """Transforms a canonical path to a form compatible with the IOAdapter.

    For example, we may convert "absolute" flow-style paths like '/some/file.py'
    to a local, relative path like 'playground/some/file.py'.
    """
        pass

    @abstractmethod
    @contextmanager
    def _reading(self, path: str, mode: str) -> IO:
        pass

    @abstractmethod
    @contextmanager
    def _writing(self, path: str, mode: str) -> IO:
        pass

    @abstractmethod
    def _makedirs(self, path: str) -> None:
        pass

    @abstractmethod
    def _glob(self, path: AbsolutePath) -> List[AbsolutePath]:
        pass

    @abstractmethod
    def _exist(self, paths: List[AbsolutePath]) -> List[bool]:
        pass

    @abstractmethod
    def _download(self, path: str) -> AbsolutePath:
        pass

    @abstractmethod
    def _upload(self, local_path: str, remote_path: str) -> None:
        pass


# LocalFSAdapter

# from glob import glob as localfs_glob
from builtins import open as localfs_open

# from os.path import exists as localfs_exists
# from os.path import normpath as localfs_normpath
# from os.path import dirname as localfs_dirname
# from os import makedirs as localfs_makedirs
#
#
# class LocalFSAdapter(IOAdapter):
#     def normpath(self, path: str) -> str:
#         path = localfs_normpath(path)
#         return path
#
#     @contextmanager
#     def _reading(self, path: str, mode: str = "r+b") -> IO:
#         reading_file = localfs_open(path, mode=mode)
#         yield reading_file
#         reading_file.close()
#
#     @contextmanager
#     def _writing(self, path: str, mode: str = "w+b") -> IO:
#         writing_file = localfs_open(path, mode=mode)
#         yield writing_file
#         writing_file.close()
#
#     def _makedirs(self, path: str) -> None:
#         dirpath = localfs_dirname(path)
#         localfs_makedirs(dirpath, exist_ok=True)
#
#     def _glob(self, glob_path: str) -> List[str]:
#         logging.debug(glob_path)
#         paths = localfs_glob(glob_path)
#         logging.debug(str(paths))
#         # we need to remove the root dir from all returned paths!
#
#         return paths  # [path[prefix_length:] for path in paths]
#
#     def _exist(self, paths: List[str]) -> List[bool]:
#         return [localfs_exists(path) for path in paths]
#
#     def _download(self, path: str) -> str:
#         """No-op on local fs. Returns LOCAL path!"""
#         return path
#
#     def _upload(self, local_path: str, remote_path: str) -> None:
#         raise NotImplementedError("local fs has no concept of uploading")


# GCStorageAdapter

from google.cloud import storage
from google.cloud.exceptions import NotFound
from tempfile import SpooledTemporaryFile, mkdtemp


class GCStorageAdapter(IOAdapter):

    _file_list: Optional[FileList]
    _bucket: Optional[Any]

    def __init__(
        self, project: str = "brain-deepviz", bucket: str = "lucid-flow"
    ) -> None:
        self.project_name = project
        self.bucket_name = bucket
        self.tempdir = AbsolutePath(mkdtemp())
        self._file_list = None
        self._bucket = None

    @property
    def bucket(self) -> Any:
        if not self._bucket:
            self._client = storage.Client(project=self.project_name)
            self._bucket = self._client.bucket(self.bucket_name)
        return self._bucket

    @property
    def file_list(self) -> FileList:
        if not self._file_list:
            self._file_list = FileList(
                project=self.project_name, bucket=self.bucket_name
            )
            self._file_list._get_all_gcs_files()
        return self._file_list

    def normpath(self, path: str) -> AbsolutePath:
        # logging.debug(f"normpathing: {path}")
        if path.startswith("gs://"):
            path = path[5:]
        # logging.debug(f"removed gs scheme: {path}")
        if path.startswith(self.bucket_name):
            path = path[len(self.bucket_name) :]
        # logging.debug(f"removed bucket: {path}")
        # if path.startswith("/"):
        # path = path[1:]
        # logging.debug(f"removed leading slash: {path}")
        return AbsolutePath(path)

    @contextmanager
    def _reading(self, path: AbsolutePath, mode: str = "r+b") -> IO:
        local_path = self._download(path)
        reading_file = localfs_open(local_path, mode=mode)
        yield reading_file
        reading_file.close()

    @contextmanager
    def _writing(self, path: AbsolutePath, mode: str = "w+b") -> IO:
        blob = storage.blob.Blob(path.as_relative_path(), self.bucket)
        local_path = self.tempdir.append(path.as_relative_path())
        makedirs(dirname(local_path), exist_ok=True)
        writing_file = localfs_open(local_path, mode=mode)
        yield writing_file
        writing_file.close()
        blob.upload_from_filename(local_path)

    def _makedirs(self, path: str) -> None:
        pass

    def _glob(self, glob_path: AbsolutePath) -> List[AbsolutePath]:
        fields = "items/name,items/updated,nextPageToken"
        matched_paths: List[AbsolutePath] = []
        # GCS returns folders iff a trailing slash is specified, so we try both:
        if glob_path.endswith("/"):
            other_path = AbsolutePath(glob_path[:-1])
        else:
            other_path = AbsolutePath(glob_path + "/")
        for glob_string in [glob_path, other_path]:
            # prefix = glob_string.split('*')[0] # == entire string if no '*' found
            # bucket_listing = self.bucket.list_blobs(fields=fields, prefix=prefix)
            # file_paths = [blob.name for blob in bucket_listing]
            # file_paths = self.file_list.glob(glob_string)
            # matched_paths += fnmatch.filter(file_paths, glob_string)
            matched_paths += self.file_list.glob(glob_string)
        # matched_paths = list(sorted(set(matched_paths)))  # should already be unique
        return matched_paths

    def _exist(self, paths: List[AbsolutePath]) -> List[bool]:
        return [self.file_list.exists(path) for path in paths]

    def _download(self, path: AbsolutePath) -> AbsolutePath:
        local_path = self.tempdir.append(path.as_relative_path())
        makedirs(dirname(local_path), exist_ok=True)
        blob = storage.blob.Blob(path.as_relative_path(), self.bucket)
        blob.download_to_filename(local_path)
        return local_path

    def _upload(self, local_path: str, remote_path: RelativePath) -> None:
        assert not remote_path.startswith("/")
        blob = storage.blob.Blob(remote_path, self.bucket)
        blob.upload_from_filename(local_path)


from os import getenv

io: IOAdapter
# if getenv("USE_LOCAL_FS", "").startswith("TRUE"):
#     logging.warn("Using LocalFSAdapter!")
#     io = LocalFSAdapter()
# else:
io = GCStorageAdapter()
