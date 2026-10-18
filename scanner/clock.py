"""Cycle boundaries, and the clock they are measured against.

Everything here is UTC, deliberately. A cycle's start time goes into every
filename the vantage point uploads, and the collection server groups those
uploads into 30-minute windows by parsing it - so two vantage points in
different timezones must not disagree about when a cycle began. The machine's
local time never enters into it.

Boundaries are measured from midnight, so the whole fleet agrees on them
without coordinating. An interval that doesn't divide the day evenly leaves a
short final interval before midnight; that is the cost of having boundaries be
a function of the clock rather than of when each process happened to start.
"""

import datetime


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def floor(dt, delta):
    """The most recent multiple of `delta` at or before `dt`."""
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt - (dt - midnight) % delta


def ceil(dt, delta):
    """The next multiple of `delta` at or after `dt`."""
    floored = floor(dt, delta)
    return floored if floored == dt else floored + delta
