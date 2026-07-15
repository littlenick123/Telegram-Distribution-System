from __future__ import annotations

from datetime import date, datetime, timezone
import tempfile
import unittest
from pathlib import Path

from app.db import Database


class DatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "test.sqlite3")
        self.db.initialize()
        self.db.add_source(-1001, "源频道", 100)
        self.db.add_route(-1001, -2001, "目标一")
        self.db.add_route(-1001, -2002, "目标二")

    def tearDown(self) -> None:
        self.db.close()
        self.tempdir.cleanup()

    def ingest(self, grouped_id: int, ids: list[int], minute: int = 0) -> tuple[int, bool]:
        return self.db.ingest_album(
            -1001,
            grouped_id,
            ids,
            datetime(2026, 7, 15, 1, minute, tzinfo=timezone.utc),
        )

    def test_album_fans_out_to_every_enabled_target(self) -> None:
        album_id, created = self.ingest(500, [102, 101])
        self.assertTrue(created)
        self.assertGreater(album_id, 0)
        inventory = self.db.inventory()
        self.assertEqual([row["pending_count"] for row in inventory], [1, 1])

    def test_duplicate_album_is_idempotent(self) -> None:
        first_id, first_created = self.ingest(500, [101, 102])
        second_id, second_created = self.ingest(500, [101, 102])
        self.assertTrue(first_created)
        self.assertFalse(second_created)
        self.assertEqual(first_id, second_id)
        self.assertEqual(self.db.status_counts(), {"pending": 2})

    def test_disabled_route_does_not_receive_new_delivery(self) -> None:
        self.db.set_route_enabled(-2002, False)
        self.ingest(500, [101, 102])
        self.assertEqual(self.db.inventory(-2001)[0]["pending_count"], 1)
        self.assertEqual(self.db.inventory(-2002)[0]["pending_count"], 0)

    def test_fifo_and_occurrence_eligibility(self) -> None:
        first_album, _ = self.ingest(500, [101, 102], minute=0)
        self.ingest(501, [103, 104], minute=30)
        route = self.db.get_route_by_target(-2001)
        row = self.db.claim_next_delivery(
            route["id"], datetime(2026, 7, 15, 1, 10, tzinfo=timezone.utc)
        )
        self.assertEqual(row["album_id"], first_album)
        self.db.mark_delivery(row["id"], "sent", target_message_ids=[900, 901])
        none_yet = self.db.claim_next_delivery(
            route["id"], datetime(2026, 7, 15, 1, 20, tzinfo=timezone.utc)
        )
        self.assertIsNone(none_yet)
        second = self.db.claim_next_delivery(
            route["id"], datetime(2026, 7, 15, 2, 0, tzinfo=timezone.utc)
        )
        self.assertIsNotNone(second)

    def test_deleted_member_cancels_all_pending_copies(self) -> None:
        self.ingest(500, [101, 102])
        cancelled = self.db.cancel_albums_containing(-1001, [102])
        self.assertEqual(cancelled, 1)
        self.assertEqual(self.db.status_counts(), {"cancelled": 2})
        self.assertEqual([row["pending_count"] for row in self.db.inventory()], [0, 0])

    def test_recover_sending_as_ambiguous(self) -> None:
        self.ingest(500, [101, 102])
        route = self.db.get_route_by_target(-2001)
        claimed = self.db.claim_next_delivery(
            route["id"], datetime(2026, 7, 16, tzinfo=timezone.utc)
        )
        self.assertIsNotNone(claimed)
        self.assertEqual(self.db.recover_interrupted(), 1)
        self.assertEqual(self.db.delivery_counts(-2001), {"ambiguous": 1})
        self.assertTrue(self.db.retry_delivery(claimed["id"]))
        self.assertEqual(self.db.delivery_counts(-2001), {"pending": 1})

    def test_inventory_threshold_is_per_target(self) -> None:
        self.ingest(500, [101, 102])
        self.assertTrue(self.db.set_threshold(-2001, 1))
        self.assertTrue(self.db.set_threshold(-2002, 8))
        rows = {row["target_telegram_id"]: row for row in self.db.inventory()}
        self.assertEqual(rows[-2001]["threshold"], 1)
        self.assertEqual(rows[-2002]["threshold"], 8)

    def test_route_target_is_unique_across_sources(self) -> None:
        self.db.add_source(-1002, "另一个源", 0)
        with self.assertRaisesRegex(ValueError, "另一个源频道"):
            self.db.add_route(-1002, -2001, "目标一")

    def test_batch_schedules_filter_and_delete_by_target(self) -> None:
        results = self.db.add_schedules(
            -2001,
            [("09:00", date(2026, 7, 14)), ("13:00", date(2026, 7, 14))],
        )
        self.assertEqual([(hhmm, created) for _, hhmm, created in results], [("09:00", True), ("13:00", True)])

        duplicate = self.db.add_schedules(-2001, [("09:00", date(2026, 7, 14))])
        self.assertFalse(duplicate[0][2])
        self.db.add_schedule(-2002, "20:00", date(2026, 7, 14))
        self.assertEqual(
            [row["time_hhmm"] for row in self.db.list_schedules(target_id=-2001)],
            ["09:00", "13:00"],
        )

        deleted, missing = self.db.delete_schedules_by_target(-2001, ["09:00", "18:00"])
        self.assertEqual(deleted, ["09:00"])
        self.assertEqual(missing, ["18:00"])
        deleted_all, missing_all = self.db.delete_schedules_by_target(-2001, None)
        self.assertEqual(deleted_all, ["13:00"])
        self.assertEqual(missing_all, [])
        self.assertEqual(len(self.db.list_schedules(target_id=-2002)), 1)

    def test_route_backfill_is_idempotent_and_excludes_cancelled_albums(self) -> None:
        self.db.set_route_enabled(-2002, False)
        first_album, _ = self.ingest(500, [101, 102])
        first_route = self.db.get_route_by_target(-2001)
        first_delivery = self.db.claim_next_delivery(
            first_route["id"], datetime(2026, 7, 16, tzinfo=timezone.utc)
        )
        self.db.mark_delivery(first_delivery["id"], "sent", target_message_ids=[900, 901])

        created, existing, pending = self.db.backfill_route(-2002)
        self.assertEqual((created, existing, pending), (1, 0, 1))
        self.assertEqual(self.db.backfill_route(-2002), (0, 1, 1))

        cancelled_album, _ = self.ingest(501, [103, 104], minute=30)
        self.db.cancel_album(cancelled_album, "测试取消")
        self.assertEqual(self.db.backfill_route(-2002), (0, 1, 1))
        self.assertEqual(self.db.delivery_counts(-2001), {"cancelled": 1, "sent": 1})


if __name__ == "__main__":
    unittest.main()
