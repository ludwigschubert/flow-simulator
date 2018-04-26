output = "/data/greetings/{name_file}-{salutation_file}-{x}.txt"

# model = "/some/filepath/"
x = [1,2,3]
name = "/data/names/{name_file}.txt"
salutation = "/data/salutations/{salutation_file}.txt"

def main() -> str:
  return "{salutation} {name}!".format(salutation=salutation, name=name)
