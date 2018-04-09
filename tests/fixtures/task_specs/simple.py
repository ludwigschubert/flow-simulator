x = [1,2,3]
name = "tests/fixtures/data/names/*.txt"
output = "tests/fixtures/data/salutations/{name}-{x}.txt"

def main():
  return "Hello {name} for the {x} time!".format(name=name, x=x)
  # with open(output, 'w') as output_file:
    # output_file.write(string)
