from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone
import logging
from typing import Awaitable, Callable

from .db import Database
from .timeutils import SERVICE_TIMEZONE, due_dates, utc_now, validate_hhmm

logger = logging.getLogger(__name__)


PublishCallback = Callable[[object, datetime], Awaitable[None]]
AlertCallback = Callable[[list[object], date], Awaitable[None]]


class ScheduleRunner:
    def __init__(
        self,
        db: Database,
        publish: PublishCallback,
        send_alerts: AlertCallback,
        *,
        poll_seconds: float = 15.0,
        clock: Callable[[], datetime] = utc_now,
    ):
        self.db = db
        self.publish = publish
        self.send_alerts = send_alerts
        self.poll_seconds = poll_seconds
        self.clock = clock
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception("调度循环异常")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass

    async def run_once(self) -> None:
        now = self.clock()
        occurrences: list[tuple[datetime, object, date]] = []
        for slot in self.db.list_schedules(enabled_only=True):
            last = date.fromisoformat(slot["last_processed_date"])
            for day, occurrence in due_dates(last, slot["time_hhmm"], now):
                occurrences.append((occurrence, slot, day))
        occurrences.sort(key=lambda item: (item[0], item[1]["id"]))

        for occurrence, slot, day in occurrences:
            try:
                await self.publish(slot, occurrence.astimezone(timezone.utc))
            finally:
                self.db.mark_schedule_processed(slot["id"], day)

        await self._run_alert_check(now)

    async def _run_alert_check(self, now: datetime) -> None:
        local_now = now.astimezone(SERVICE_TIMEZONE)
        alert_time = validate_hhmm(self.db.get_setting("alert_time", "09:00") or "09:00")
        if local_now.strftime("%H:%M") < alert_time:
            return
        checked = self.db.get_setting("last_alert_check_date")
        if checked == local_now.date().isoformat():
            return
        low = [
            row
            for row in self.db.inventory()
            if row["alert_enabled"]
            and row["route_enabled"]
            and row["source_enabled"]
            and row["pending_count"] < row["threshold"]
        ]
        if low:
            await self.send_alerts(low, local_now.date())
            self.db.mark_alerted([int(row["route_id"]) for row in low], local_now.date())
        self.db.set_setting("last_alert_check_date", local_now.date().isoformat())
