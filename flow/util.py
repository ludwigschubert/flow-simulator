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
