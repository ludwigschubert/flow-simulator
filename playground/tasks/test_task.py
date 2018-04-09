from task_spec import *

x = LiteralList([1,2,3])
name = FileGlob("/names/*.txt")

output = Out("/outputs/{name}-{x}.txt")

#####

x = [1,2,3]
name = "/names/*.txt"
model = "/models/*.modelzoo"
layer = "{model}.layer"
output = "/putoutps/{name}-{x}.txt"

def main():
    import tensorflow as tf
    return "Hello {name}!" % {"name": name}
    