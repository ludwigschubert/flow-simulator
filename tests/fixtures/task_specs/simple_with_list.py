x = [1,2,3]
name = "/tests/fixtures/data/names/{name_id}.txt"
output = "/tests/fixtures/data/salutations/{name_id}-{x}.txt"

def main():
  return "Hello {name} for the {x} time!".format(name=name, x=x)
