from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

def utc_to_ist(unix_ts):
    # First interpret timestamp as UTC
    dt_utc = datetime.fromtimestamp(unix_ts, tz=timezone.utc)

    # Convert to IST
    dt_ist = dt_utc.astimezone(IST)

    return dt_ist