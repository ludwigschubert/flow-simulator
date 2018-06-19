
from typing import Any
from re import match, compile
from os.path import dirname, basename, join


class Path(str):
    @property
    def basename(self) -> "RelativePath":
        return RelativePath(basename(self))

    # def as_absolute(path: str) -> "AbsolutePath":
    #     if not path.startswith("/"):
    #         path


class AbsolutePath(Path):
    def __init__(self, path: str) -> None:
        if not path.startswith("/"):
            raise ValueError(f"AbsolutePath `{path}` has to start with '/'!")
        if ":" in path:
            raise ValueError(
                f"AbsolutePath `{path}` must not contain a schema separated by ':'!"
            )
        super()

    @property
    def dirname(self) -> "AbsolutePath":
        return AbsolutePath(dirname(self))

    def as_relative_path(self) -> "RelativePath":
        """Re-interpret an absolute path as relative."""
        return RelativePath(self[1:])

    def append(self, suffix: "RelativePath") -> "AbsolutePath":
        return AbsolutePath(join(self, suffix))


class RelativePath(Path):
    def __init__(self, path: str) -> None:
        if path.startswith("/"):
            raise ValueError(f"RelativePath `{path}` must not start with '/'!")
        if ":" in path:
            raise ValueError(
                f"RelativePath `{path}` must not contain a schema separated by ':'!"
            )
        super()

    @property
    def dirname(self) -> "RelativePath":
        return RelativePath(dirname(self))

    def prepend(self, prefix: AbsolutePath) -> AbsolutePath:
        return AbsolutePath(join(prefix, self))


class AbsoluteURL(Path):
    regex = compile(r"\w+://.+")

    def __init__(self, url: str) -> None:
        if not self.regex.match(url):
            raise ValueError(f"'{url}' does not match AbsoluteURL regex!")
        super()

    @property
    def dirname(self) -> "AbsoluteURL":
        return AbsoluteURL(dirname(self))

    def to_path(self) -> AbsolutePath:
        raise NotImplementedError


class AbsoluteGCSURL(AbsoluteURL):
    regex = compile(r"\w+://(\w+)/.+")

    # @property
    # def bucket_name(self) -> str:
    # matches = regex.match(self)
    # assert(matches)

    def from_absolute_path(path: AbsolutePath) -> "AbsoluteGCSURL":
        return AbsoluteGCSURL("gs://lucid-flow" + path)

    def to_path(self) -> AbsolutePath:
        raise NotImplementedError


ROOT: AbsolutePath = AbsolutePath("/")
