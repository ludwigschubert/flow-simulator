from types import ModuleType
import importlib
from uuid import uuid4
from os.path import exists

def import_module_from_local_source(file_path: str) -> ModuleType:
  if not exists(file_path):
    raise RuntimeError("Attempt at importing %s failed because no file was found at that path!", file_path)
  module_name = 'task_specification_' + str(uuid4())
  spec = importlib.util.spec_from_file_location(module_name, file_path)
  module = importlib.util.module_from_spec(spec)
  loader = spec.loader
  if loader:
    loader.exec_module(module)
    return module
  else:
    raise RuntimeError("Attempt at importing %s did not return a valid module. :/", file_path)
