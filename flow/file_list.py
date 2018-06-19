""" FileList manages an in-memory cache of existing files on a GCS bucket.

It consists of:
* a datastructure that allows:
    * fast existence checks via a hashset
    * globbing via a simple list
* a helper function for getting all files off a GCS bucket

The idea is roughly that a server can get all files from GCS and then use pubsub
to keep up to date with what files exist. This is not yet implemented.
"""
from google.cloud import storage
from google.cloud.exceptions import NotFound

from flow.path import RelativePath, AbsolutePath, ROOT
import fnmatch
from typing import List, Set


class FileList(object):

    paths: List[AbsolutePath]
    path_set: Set[AbsolutePath]

    def __init__(
        self,
        project: str = "brain-deepviz",
        bucket: str = "lucid-flow",
        paths: List[AbsolutePath] = [],
    ) -> None:
        self.project_name = project
        self.bucket_name = bucket
        self.paths = paths
        self.path_set = set(paths)
        # self._get_all_gcs_files()

    def glob(self, glob_string: AbsolutePath) -> List[AbsolutePath]:
        if not isinstance(glob_string, AbsolutePath):
            raise ValueError("Can only use AbsolutePath objects with FileList!")
        paths = fnmatch.filter(self.paths, glob_string)
        return [AbsolutePath(path) for path in paths]

    def exists(self, file_path: AbsolutePath) -> bool:
        if not isinstance(file_path, AbsolutePath):
            raise ValueError("Can only use AbsolutePath objects with FileList!")
        return file_path in self.path_set

    def _get_all_gcs_files(self) -> None:
        client = storage.Client(project=self.project_name)
        bucket = client.bucket(self.bucket_name)
        fields = "items/name,nextPageToken"
        listing = bucket.list_blobs(fields=fields)
        self.paths = [ROOT.append(RelativePath(blob.name)) for blob in listing]
        self.path_set = set(self.paths)

    def add(self, file_path: AbsolutePath) -> None:
        if not isinstance(file_path, AbsolutePath):
            raise ValueError("Can only use AbsolutePath objects with FileList!")
        self.paths.append(file_path)
        self.path_set |= set([file_path])

    def remove(self, file_path: AbsolutePath) -> None:
        raise NotImplementedError


_file_list = None


def get_file_list() -> FileList:
    if not _file_list:
        _file_list = FileList()
    return _file_list
