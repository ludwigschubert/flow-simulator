import pytest

from flow.path import RelativePath, AbsolutePath, AbsoluteURL, AbsoluteGCSURL


def test_relative_path():
    rp = RelativePath("a/relative/path.ext")


def test_relative_path_absolute():
    with pytest.raises(ValueError):
        rp = RelativePath("/an/absolute/path.ext")


def test_relative_path_absolute_url():
    with pytest.raises(ValueError):
        rp = RelativePath("gs://an/absolute/url.ext")


def test_absolute_path():
    ap = AbsolutePath("/an/absolute/path.ext")


def test_absolute_path_relative():
    with pytest.raises(ValueError):
        rp = AbsolutePath("a/relative/path.ext")


def test_absolute_path_url():
    with pytest.raises(ValueError):
        rp = AbsolutePath("gs://an/absolute/url.ext")


def test_absolute_url():
    au = AbsoluteURL("gs://an/absolute/url.ext")


def test_absolute_url_relative():
    with pytest.raises(ValueError):
        rp = AbsoluteURL("a/relative/path.ext")


def test_absolute_url_path():
    with pytest.raises(ValueError):
        rp = AbsoluteURL("/an/absolute/path.ext")


def test_absolute_gcs_url():
    agcsu1 = AbsoluteGCSURL("gs://bucket/an/absolute/path.ext")
    agcsu2 = AbsoluteGCSURL(
        "gs://lucid-flow/data/evaluations/task=feature-visualization/model=mobilenet/layer=MobilenetV1\MobilenetV1\Conv2d_6_depthwise\Relu6/channel=0366/objective=channel/image.png"
    )


# dirname


def test_relative_path_dirname():
    rp = RelativePath("a/relative/path.ext")
    dir = rp.dirname
    assert dir == "a/relative"


def test_absolute_path_dirname():
    ap = AbsolutePath("/an/absolute/path.ext")
    dir = ap.dirname
    assert dir == "/an/absolute"


def test_absolute_url_dirname():
    au = AbsoluteURL("gs://bucket/an/absolute/path.ext")
    dir = au.dirname
    assert dir == "gs://bucket/an/absolute"


# basename


def test_relative_path_basename():
    rp = RelativePath("a/relative/path.ext")
    dir = rp.basename
    assert dir == "path.ext"


def test_absolute_path_basename():
    ap = AbsolutePath("/an/absolute/path.ext")
    dir = ap.basename
    assert dir == "path.ext"


def test_absolute_url_basename():
    au = AbsoluteURL("gs://bucket/an/absolute/path.ext")
    dir = au.basename
    assert dir == "path.ext"
