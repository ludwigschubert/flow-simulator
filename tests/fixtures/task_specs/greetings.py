# x = [1,2,3]
name = "data/names/{name}.txt"
salutation = "data/salutations/{salutation}.txt"
output = "data/greetings/{name}-{salutation}.txt"

def main():
  return "{salutation} {name}!".format(salutation=salutation, name=name)
