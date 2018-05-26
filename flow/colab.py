import tempfile
import os.path as osp
import uuid
from typing import List
import numpy as np


from IPython.core.magic import register_cell_magic
from flow.task_parser import TaskParser

_temp_dir = tempfile.mkdtemp(prefix="flow_")

@register_cell_magic
def flow_task_spec(line: str, cell: str) -> None:
  base_name = line.split()[0]
  filename = osp.join(_temp_dir, f"{base_name}.py")
  with open(filename, "w") as file_handle:
    file_handle.write(cell)
  task_spec = TaskParser(filename).to_spec()
  globals()[base_name] = task_spec
