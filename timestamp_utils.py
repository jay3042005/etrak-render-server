"""Serialize database timestamps without depending on the server's local zone."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional


def format_datetime_utc(value) -> Optional[str]:
    """Return timezone-aware ISO 8601 for SQLite strings or UTC instants.

    SQLite CURRENT_TIMESTAMP returns a timezone-less UTC string. MySQL report
    queries supply UNIX_TIMESTAMP(created_at), because its TIMESTAMP columns
    otherwise become naive datetimes in the connection's session timezone.
    """
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        value = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, (str, bytes)):
        text = value.decode() if isinstance(value, bytes) else value
        text = text.strip()
        # An epoch may arrive as text (some drivers stringify DECIMAL).
        try:
            value = datetime.fromtimestamp(float(text), tz=timezone.utc)
        except ValueError:
            # Python 3.10 (embedded Android runtime) does not accept a trailing Z.
            value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise TypeError("Expected a database datetime, ISO string, or Unix timestamp")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def report_created_at_column(dialect: str) -> str:
    """Select a report's creation instant without changing DB/session timezones.

    Using an epoch for MySQL TIMESTAMP avoids incorrectly tagging a session's
    local wall-clock datetime as UTC. SQLite's CURRENT_TIMESTAMP is already UTC.
    The alias is private to the query; the API still exposes only created_at.
    """
    if dialect in {"mysql", "mariadb"}:
        return "UNIX_TIMESTAMP(created_at) AS created_at_utc"
    return "created_at AS created_at_utc"


def truck_last_updated_column(dialect: str) -> str:
    """Select a truck's last GPS instant as a true UTC epoch on MySQL.

    `truck_locations.last_updated` is a MySQL TIMESTAMP written by
    CURRENT_TIMESTAMP, which MariaDB stores and returns in the *session* time
    zone (SYSTEM here, i.e. Asia/Manila). Reading it straight into Python gives
    a naive datetime whose wall clock is Manila time. `format_datetime_utc` then
    stamps it `+00:00`, so the API reported GPS fixes 8 hours in the future and
    the read-time staleness check in `_effective_truck_status` could never
    expire a truck.

    UNIX_TIMESTAMP() asks MySQL for the real instant instead, converting the
    stored local wall clock to a UTC epoch using the session zone. That is the
    same approach `report_created_at_column` already uses for reports, so both
    surfaces now agree.

    SQLite stores CURRENT_TIMESTAMP as UTC already, so it needs no conversion.

    Aliased to `last_updated` so the existing row mapping and serializers keep
    working unchanged.
    """
    if dialect in {"mysql", "mariadb"}:
        return "UNIX_TIMESTAMP(last_updated) AS last_updated"
    return "last_updated AS last_updated"
