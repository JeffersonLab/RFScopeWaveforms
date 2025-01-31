from datetime import datetime, timezone

def get_datetime_as_utc(dt: datetime):
    """Convert a datetime to UTC timezone.  Assume it is system local system timezone if no time zone info given."""
    if dt.tzinfo is None:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=timezone.utc)
