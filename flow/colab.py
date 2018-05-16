import tempfile
import os.path as osp
import uuid

from IPython.core.magic import register_cell_magic
from flow.task_parser import TaskParser

_temp_dir = tempfile.mkdtemp(prefix="flow_")

@register_cell_magic
def flow_task_spec(line, cell):
  base_name = line.split()[0]
  name_str = base_name + "_" + str(uuid.uuid4())
  filename = osp.join(_temp_dir, f"{name_str}.py")
  with open(filename, "w") as file_handle:
    file_handle.write(cell)
  task_spec = TaskParser(filename).to_spec()
  globals()[base_name] = task_spec
