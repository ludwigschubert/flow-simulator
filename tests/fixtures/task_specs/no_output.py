x = [1,2,3]
name = "/names/{name}.txt"

def main():
    import tensorflow as tf
    return "Hello {name}!" % {"name": name}
