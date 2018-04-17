output = "/data/greetings/{name}-{salutation}-{x}.txt"
model = "/some/filepath/"
x = [1,2,3]
name = "/data/names/{name}.txt"
salutation = "/data/salutations/{salutation}.txt"

def main():
  return "{salutation} {name}!".format(salutation=salutation, name=name)
