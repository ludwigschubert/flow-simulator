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


from functools import partial

class memoize(object):
  """cache the return value of a method

  This class is meant to be used as a decorator of methods. The return value
  from a given method invocation will be cached on the instance whose method
  was invoked. All arguments passed to a method decorated with memoize must
  be hashable.

  If a memoized method is invoked directly on its class the result will not
  be cached. Instead the method will be invoked like a static method:
  class Obj(object):
      @memoize
      def add_to(self, arg):
          return self + arg
  Obj.add_to(1) # not enough arguments
  Obj.add_to(1, 2) # returns 3, result is not cached
  via http://code.activestate.com/recipes/577452
  """
  def __init__(self, func):
    self.func = func
  def __get__(self, obj, objtype=None):
    if obj is None:
      return self.func
    return partial(self, obj)
  def __call__(self, *args, **kw):
    obj = args[0]
    try:
      cache = obj.__cache
    except AttributeError:
      cache = obj.__cache = {}
    key = (self.func, args[1:], frozenset(kw.items()))
    try:
      res = cache[key]
    except KeyError:
      res = cache[key] = self.func(*args, **kw)
    return res
