from __future__ import annotations

from datetime import date, datetime, timezone
import tempfile
import unittest
from pathlib import Path

from app.db import Database
from app.scheduler import ScheduleRunner
from app.timeutils import SERVICE_TIMEZONE, due_dates, initial_schedule_date, validate_hhmm


class TimeUtilityTests(unittest.TestCase):
    def test_validate_hhmm(self) -> None:
        self.assertEqual(validate_hhmm("09:05"), "09:05")
        with self.assertRaises(ValueError):
            validate_hhmm("25:00")

    def test_due_dates_include_all_missed_days(self) -> None:
        now = datetime(2026, 7, 15, 10, 0, tzinfo=SERVICE_TIMEZONE)
        result = due_dates(date(2026, 7, 12), "09:00", now)
        self.assertEqual([day for day, _ in result], [date(2026, 7, 13), date(2026, 7, 14), date(2026, 7, 15)])

    def test_new_late_schedule_does_not_fire_retroactively(self) -> None:
        now = datetime(2026, 7, 15, 10, 0, tzinfo=SERVICE_TIMEZONE)
        self.assertEqual(initial_schedule_date("09:00", now), date(2026, 7, 15))
        self.assertEqual(initial_schedule_date("11:00", now), date(2026, 7, 14))


class ScheduleRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "test.sqlite3")
        self.db.initialize()
        self.db.add_source(-1001, "源频道", 10)
        self.db.add_route(-1001, -2001, "目标频道")

    async def asyncTearDown(self) -> None:
        self.db.close()
        self.tempdir.cleanup()

    async def test_runner_replays_every_missed_slot(self) -> None:
        self.db.add_schedule(-2001, "09:00", date(2026, 7, 12))
        calls: list[datetime] = []

        async def publish(slot: object, occurrence: datetime) -> None:
            calls.append(occurrence)

        async def alerts(rows: list[object], day: date) -> None:
            pass

        now = datetime(2026, 7, 15, 10, 0, tzinfo=SERVICE_TIMEZONE)
        runner = ScheduleRunner(self.db, publish, alerts, clock=lambda: now)
        await runner.run_once()
        self.assertEqual(len(calls), 3)
        await runner.run_once()
        self.assertEqual(len(calls), 3)

    async def test_low_stock_alerts_once_per_day_and_strictly_below(self) -> None:
        self.db.ingest_album(
            -1001,
            100,
            [11, 12],
            datetime(2026, 7, 15, tzinfo=timezone.utc),
        )
        self.db.set_threshold(-2001, 1)
        alert_calls: list[list[object]] = []

        async def publish(slot: object, occurrence: datetime) -> None:
            pass

        async def alerts(rows: list[object], day: date) -> None:
            alert_calls.append(rows)

        now = datetime(2026, 7, 15, 9, 0, tzinfo=SERVICE_TIMEZONE)
        runner = ScheduleRunner(self.db, publish, alerts, clock=lambda: now)
        await runner.run_once()
        self.assertEqual(alert_calls, [])  # remaining == threshold

        self.db.set_setting("last_alert_check_date", "2026-07-14")
        self.db.set_threshold(-2001, 2)
        await runner.run_once()
        await runner.run_once()
        self.assertEqual(len(alert_calls), 1)
        self.assertEqual(alert_calls[0][0]["pending_count"], 1)

    async def test_alert_disabled_target_is_ignored(self) -> None:
        self.db.set_alert_enabled(-2001, False)
        alert_calls: list[list[object]] = []

        async def publish(slot: object, occurrence: datetime) -> None:
            pass

        async def alerts(rows: list[object], day: date) -> None:
            alert_calls.append(rows)

        now = datetime(2026, 7, 15, 9, 0, tzinfo=SERVICE_TIMEZONE)
        await ScheduleRunner(self.db, publish, alerts, clock=lambda: now).run_once()
        self.assertEqual(alert_calls, [])


if __name__ == "__main__":
    unittest.main()
