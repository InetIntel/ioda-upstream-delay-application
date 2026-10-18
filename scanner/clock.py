"""
To more effectively bucket yarrp scans across VPs, we use the exact virtual
starting time of the scan (the exact half an hour mark) rather than the physical
time started (usually a few ms after the half an hour) so that we can easily associate
yarrp scans by the 30-minute bucket they fall into (keeps things simple server side).
Logic extends to intervals other than 30-minute if necessary.
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
