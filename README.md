# data/file/tensor/flow/ci-for-research/auto-running ~make~python files

"Those Who Do Not Learn make Are Doomed To Reimplement It."

## Overview

This is a local-filesystem simulator of flow, the CI system for research
I'm trying to build for Chris et al.
Earlier today I was wasting too much time on Google Cloud APIs, and I wanted
to have a functioning version of the outline Katherine helped me write.

## Current task_spec syntax

```python
x = [1,2,3]
name = "tests/fixtures/data/names/*.txt"
output = "tests/fixtures/data/salutations/{name}-{x}.txt"
def main():
  return "Hello {name} for the {x} time!".format(name=name, x=x)
```

## Setup

Careful: This requires **Python 3.6** or higher.

```bash
virtualenv env
source env/bin/activate
pip install -r requirements.txt
```

## Run

### Authentication

Execute
```
gcloud auth application-default login
```

Slightly awkward because the simulator depends on flow, but not explicitly.

```bash
PYTHONPATH='.' python simulator/main.py
USE_LOCAL_QUEUE=FALSE USE_LOCAL_FS=TRUE PYTHONPATH='.' python simulator/main.py
```

Start by moving `say_hello_world.py` from `playground` to `playground/tasks`.
Flow creates results in `greetings`, as specified in that task.
Then, within 'playground' subfolder, create or move new inputs in/to specified input directories.


### Right hand side cases

#### lists
just take as input
match all files

#### strings
are PathTemplates
may produce and or require implicit variables

match only when template matches

#### Lambdas
have inputs
evaluated once per product of inputs
return list of values which is a dimension

#### dictionaries
have path templates, but some implicit variables are locally bound
they produce a list of dictionaries (one per unbound variable combination)

#### output
is PathTemplate
but requires all it's implicit variables to already be bound
