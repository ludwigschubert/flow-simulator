from datetime import timedelta

def format_timedelta(timedelta: timedelta) -> str:
  """Format a timedelta object as a human readable string."""
  # prepare dictionary with time units
  d = dict()
  d['days']         = timedelta.days
  d['hours'], rem   = divmod(timedelta.seconds, 60*60)
  d['minutes'], rem = divmod(rem, 60)
  d['seconds']      = rem
  # format to two largest units of accuracy
  if d['minutes'] == 0:
    template = '{seconds}s'
  elif d['hours'] == 0:
    template = '{minutes}m,{seconds}s'
  elif d['days'] == 0:
    template = '{hours}h:{minutes}m'
  elif d['days'] == 1:
    template = '{days} day, {hours}h'
  else:
    template = '{days} days, {hours}h'
  return template.format(**d)

def memoize_single_arg(f):
  """ Memoization decorator for a function taking a single argument.
  Via http://code.activestate.com/recipes/578231"""
  class memodict(dict):
    def __missing__(self, key):
      ret = self[key] = f(key)
      return ret
  return memodict().__getitem__

def memoize(f):
  """ Memoization decorator for functions taking one or more arguments.
  Via https://wiki.python.org/moin/PythonDecoratorLibrary
  """
  class memodict(dict):
    def __init__(self, f):
      self.f = f
    def __call__(self, *args):
      return self[args]
    def __missing__(self, key):
      ret = self[key] = self.f(*key)
      return ret
  return memodict(f)
