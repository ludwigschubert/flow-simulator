name = "/tests/fixtures/data/names/{name}.txt"
output = "/tests/fixtures/data/salutations/{name}.txt"

def main():
  return "Hello {name}!".format(name=name)
