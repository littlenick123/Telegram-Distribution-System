from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

SERVICE_TIMEZONE = ZoneInfo("Asia/Shanghai")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def validate_hhmm(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%H:%M")
    except ValueError as exc:
        raise ValueError("时间必须使用 HH:MM（24 小时制）") from exc
    return parsed.strftime("%H:%M")


def local_occurrence(day: date, hhmm: str) -> datetime:
    parsed = time.fromisoformat(validate_hhmm(hhmm))
    return datetime.combine(day, parsed, tzinfo=SERVICE_TIMEZONE)


def due_dates(last_processed: date, hhmm: str, now: datetime) -> list[tuple[date, datetime]]:
    local_now = now.astimezone(SERVICE_TIMEZONE)
    current = last_processed + timedelta(days=1)
    result: list[tuple[date, datetime]] = []
    while current <= local_now.date():
        occurrence = local_occurrence(current, hhmm)
        if occurrence <= local_now:
            result.append((current, occurrence))
        current += timedelta(days=1)
    return result


def initial_schedule_date(hhmm: str, now: datetime) -> date:
    """Avoid retroactively firing a schedule that is created after its time today."""
    local_now = now.astimezone(SERVICE_TIMEZONE)
    if local_occurrence(local_now.date(), hhmm) <= local_now:
        return local_now.date()
    return local_now.date() - timedelta(days=1)

