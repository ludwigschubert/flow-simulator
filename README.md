# flow-simulator

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

```
@task("playground/data/greetings/hello-{name}-{salutation}.txt")
def greeting(name="playground/data/names/*.txt",
             salutation="playground/data/salutations/*.txt"):
```

## Setup

Careful: **Python 3** only.

```
virtualenv env
source env/bin/activate
pip install -r requirements.txt
```

## Run

```
python main.py
```

Start by dragging `say_hello_world.py` from `playground` to `playground/tasks`.
Results are created in `greetings`, as specified in that task.
Then, within 'playground' subfolder, drag new inputs to specified input directories.