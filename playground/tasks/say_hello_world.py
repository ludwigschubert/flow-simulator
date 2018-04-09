from flow.task import task

import logging
from os.path import splitext, basename, dirname
from os import makedirs

@task("playground/data/greetings/hello-{name}-{salutation}.txt")
def greeting(name={'name': "playground/data/names/*.txt"},
             salutation="playground/data/salutations/*.txt"):

  # TODO: defaults for getting the contents of files instead? lucid.io.load???
  # maybe additional syntax? e.g. "folder/*.txt:contents"

  with open(name, 'r') as file:
    the_name = file.readlines()[0].rstrip('\n')
  with open(salutation, 'r') as file:
    the_salutation = file.readlines()[0].rstrip('\n')

  # TODO: in a sense, this line is all the code that should be here:
  result = "{} {}!".format(the_salutation, the_name)

  # TODO: defaults for returning values? lucid.io.save on return value???
  # TODO: r&d supply open file handles to inputs and outputs??
  # TODO: automatically figure out unique result filename somehow??
  name_id = splitext(basename(name))[0]
  salutation_id = splitext(basename(salutation))[0]
  # TODO: autogenerate this using output_placeholder_regex from flow.task
  output_path = greeting.output_spec.replace('{name}', name_id).replace('{salutation}', salutation_id)

  makedirs(dirname(output_path), exist_ok=True)
  with open(output_path, 'w') as output_file:
    output_file.write(result)
