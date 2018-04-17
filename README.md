# data/file/tensor/flow/ci-for-research/auto-running ~make~python files

"Those Who Do Not Learn make Are Doomed To Reimplement It."

## Overview

This is a local-filesystem simulator of flow, the CI system for research
I'm trying to build for Chris et al.
Earlier today I was wasting too much time on Google Cloud APIs, and I wanted
to have a functioning version of the outline Katherine helped me write.

There is a lot of map/territory confusion in the naming conventions of this code.
Eventually I'd like things to be properly called 'task_spec' if its a specification,
rather than 'task'.

## Current task_spec syntax

```python
x = [1,2,3]
name = "tests/fixtures/data/names/*.txt"
output = "tests/fixtures/data/salutations/{name}-{x}.txt"
def main():
  return "Hello {name} for the {x} time!".format(name=name, x=x)
```

## Setup

Careful: **Python 3** only.

```bash
virtualenv env
source env/bin/activate
pip install -r requirements.txt
```

## Run

Slightly awkward because the simulator depends on flow, but not explicitly.

```bash
PYTHONPATH='.' python simulator/main.py
USE_LOCAL_QUEUE=FALSE USE_LOCAL_FS=TRUE PYTHONPATH='.' python simulator/main.py
```

Start by moving `say_hello_world.py` from `playground` to `playground/tasks`.
Results are created in `greetings`, as specified in that task.
Then, within 'playground' subfolder, create or move new inputs in/to specified input directories.
