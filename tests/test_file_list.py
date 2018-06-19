import pytest
from flow.file_list import FileList, get_file_list
from flow.path import AbsolutePath, AbsoluteURL


@pytest.fixture
def empty_file_list():
    return FileList()


def test_init_file_list(empty_file_list):
    empty_file_list


def test_file_list_add(empty_file_list, absolute_path):
    file_list = empty_file_list
    file_list.add(absolute_path)
    assert file_list.exists(absolute_path)


def test_file_list_exists(absolute_path):
    file_list = FileList(paths=[absolute_path])
    assert file_list.exists(absolute_path)


def test_file_list_glob():
    glob_path = AbsolutePath("/an/*/path.ext")
    absolute_path = AbsolutePath("/an/absolute/path.ext")
    file_list = FileList(paths=[absolute_path])
    assert absolute_path in file_list.glob(glob_path)


def test_file_list_glob_negative():
    glob_path = AbsolutePath("/a/different/*/path.ext")
    absolute_path = AbsolutePath("/an/absolute/path.ext")
    file_list = FileList(paths=[absolute_path])
    assert absolute_path not in file_list.glob(glob_path)


def test_file_list_invalid_glob_fails_loudly():
    glob_url = AbsoluteURL("gs://lucid-flow/an/*/path.ext")
    absolute_path = AbsolutePath("/an/absolute/path.ext")
    file_list = FileList(paths=[absolute_path])
    with pytest.raises(ValueError):
        file_list.glob(glob_url)


def test_file_list_fails_loudly(empty_file_list, absolute_url):
    file_list = empty_file_list
    with pytest.raises(ValueError):
        file_list.add(absolute_url)
    with pytest.raises(ValueError):
        file_list.exists(absolute_url)
    with pytest.raises(ValueError):
        file_list.glob(absolute_url)
    with pytest.raises(NotImplementedError):
        file_list.remove(absolute_url)


def test_joining_absolute_paths():
    absolute_one = AbsolutePath("/an/absolute/path.ext")
    absolute_two = AbsolutePath("/prefix/absolute/path")
    relative_one = absolute_one.as_relative_path()
    joined_1 = relative_one.prepend(absolute_two)
    joined_2 = absolute_two.append(relative_one)
    assert joined_1 == joined_2


@pytest.mark.skip(reason="Slow test because of network access")
def test_gcs_connection(empty_file_list):
    file_list = empty_file_list
    file_list._get_all_gcs_files()
    assert file_list.exists(AbsolutePath("/data/noop"))
