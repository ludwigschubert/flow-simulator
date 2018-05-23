import logging
from string import Template
from typing import Pattern, Mapping, List, Optional
from collections import defaultdict
from re import compile as re_compile

from flow.typing import Bindings, Variable, Value

class PathTemplateError(ValueError):
  pass

class PathTemplate(Template):

  delimiter = '{'
  pattern = r'{(?P<named>[^{}/]+)}'
  path_template_prefix = "gs://lucid-flow"

  def __init__(self, raw_path_template: str, already_cooked: bool = False) -> None:
    if "*" in raw_path_template:
      raise PathTemplateError("Path template ("+raw_path_template+") may not contain '*'. Use {name} instead.")
    if already_cooked:
      path_template = raw_path_template
    else:
      if not raw_path_template.startswith('/'):
        raise PathTemplateError(f"Raw PathTemplate ({raw_path_template}) must start with '/'.")
      path_template = self.path_template_prefix + raw_path_template

    super().__init__(path_template)

  def __repr__(self) -> str:
    return f"<PathTemplate prefix={self.path_template_prefix} template={self.template}>"

  @property
  def _capture_regex(self) -> Pattern[str]:
    regex = self.template \
      .replace("/", "\/") \
      .replace("{", r"(?P<") \
      .replace("}", r">[^{}/]+)")
    return re_compile(regex)

  @property
  def glob(self) -> str:
    substitution: Mapping[str, str] = defaultdict(lambda: '*')
    return self.substitute(substitution)

  @staticmethod
  def _escape_slashes_in_dict(dict: Mapping, unescape: bool = False) -> Mapping:
    mapping = {}
    for key, value in dict.items():
      if isinstance(value, str):
        if unescape:
          value = value.replace("\\", "/")
        else:
          value = value.replace("/", "\\")
      mapping[key] = value
    return mapping

  def format(self, replacements: Mapping[str, str]) -> str:
    escaped = self._escape_slashes_in_dict(replacements)
    return self.template.format(**escaped)

  @property
  def placeholders(self) -> List[Variable]:
    return self.pattern.findall(self.template)

  def with_replacements(self, replacements: Mapping[Variable, str]) -> 'PathTemplate':
    escaped = self._escape_slashes_in_dict(replacements)
    return PathTemplate(self.safe_substitute(escaped), already_cooked=True)

  def match(self, path: str) -> Optional[Mapping[str, str]]:
    # logging.debug(self._capture_regex)
    # logging.debug(path)
    match = self._capture_regex.match(str(path))
    # logging.debug(match)
    if match:
      result = match.groupdict()
      # logging.debug(result)
      escaped = self._escape_slashes_in_dict(result, unescape=True)
      # logging.debug(escaped)
      return escaped
    else:
      return None
