from typing import Tuple, Mapping, Union

Value = Union[str, int, float, Tuple[str, str]]
Variable = str
Bindings = Mapping[Variable, Value]
