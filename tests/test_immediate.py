from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from app.db import Database
from app.immediate import ImmediateSendRunner


class ImmediateSendRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "test.sqlite3")
        self.db.initialize()
        self.db.add_source(-1001, "源频道", 100)
        self.db.add_route(-1001, -2001, "目标频道")
        self.db.ingest_album(
            -1001,
            500,
            [101, 102],
            datetime(2026, 7, 15, tzinfo=timezone.utc),
        )

    async def asyncTearDown(self) -> None:
        self.db.close()
        self.tempdir.cleanup()

    async def test_terminal_job_notice_is_sent_once(self) -> None:
        job = self.db.create_immediate_send_job(-2001, 0, 1, 123)
        self.db.cancel_immediate_job(job["job_id"])
        notices: list[tuple[int, str]] = []

        async def notify(admin_id: int, text: str) -> None:
            notices.append((admin_id, text))

        runner = ImmediateSendRunner(self.db, object(), notify)
        self.assertTrue(await runner.run_once())
        self.assertEqual(notices[0][0], 123)
        self.assertIn("已取消", notices[0][1])
        self.assertFalse(await runner.run_once())


if __name__ == "__main__":
    unittest.main()
