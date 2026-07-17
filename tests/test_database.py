from __future__ import annotations

from datetime import date, datetime, timezone
import tempfile
import unittest
from pathlib import Path
import sqlite3

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

    def test_disabled_route_or_source_cannot_claim_existing_delivery(self) -> None:
        self.ingest(500, [101, 102])
        route = self.db.get_route_by_target(-2001)
        eligible = datetime(2026, 7, 16, tzinfo=timezone.utc)
        self.db.set_route_enabled(-2001, False)
        self.assertIsNone(self.db.claim_next_delivery(route["id"], eligible))
        self.db.set_route_enabled(-2001, True)
        self.db.set_source_enabled(-1001, False)
        self.assertIsNone(self.db.claim_next_delivery(route["id"], eligible))

    def test_route_delete_cascades_target_state_and_keeps_source_library(self) -> None:
        self.db.add_route(-1001, -2001, "话题群", 135, "动作电影")
        self.db.add_schedule(-2001, "09:00", date(2026, 7, 14), 135)
        self.db.set_threshold(-2001, 7, 135)
        first_album, _ = self.ingest(500, [101, 102])
        self.ingest(501, [103, 104], minute=30)
        route = self.db.get_route_by_target(-2001, 135)
        claimed = self.db.claim_next_delivery(
            route["id"], datetime(2026, 7, 16, tzinfo=timezone.utc)
        )
        self.db.mark_delivery(claimed["id"], "sent", target_message_ids=[900, 901])

        result = self.db.delete_route(-2001, 135)

        self.assertTrue(result["deleted"])
        self.assertEqual(result["delivery_counts"], {"pending": 1, "sent": 1})
        self.assertEqual(result["schedule_count"], 1)
        self.assertIsNone(self.db.get_route_by_target(-2001, 135))
        self.assertEqual(
            self.db.list_schedules(target_id=-2001, target_topic_id=135), []
        )
        self.assertEqual(
            self.db.conn.execute(
                "SELECT COUNT(*) FROM source_albums WHERE source_id = ?",
                (self.db.get_source(-1001)["id"],),
            ).fetchone()[0],
            2,
        )
        self.assertIsNotNone(
            self.db.conn.execute(
                "SELECT id FROM source_albums WHERE id = ?", (first_album,)
            ).fetchone()
        )
        self.assertEqual(self.db.delivery_counts(-2002), {"pending": 2})
        self.db.add_route(-1001, -2001, "话题群", 135, "动作电影")
        self.assertIsNotNone(self.db.get_route_by_target(-2001, 135))

    def test_route_delete_waits_for_sending_and_leaves_route_disabled(self) -> None:
        self.ingest(500, [101, 102])
        self.ingest(501, [103, 104], minute=30)
        route = self.db.get_route_by_target(-2001)
        eligible = datetime(2026, 7, 16, tzinfo=timezone.utc)
        claimed = self.db.claim_next_delivery(route["id"], eligible)

        blocked = self.db.delete_route(-2001)

        self.assertFalse(blocked["deleted"])
        self.assertEqual(blocked["delivery_counts"]["sending"], 1)
        self.assertFalse(self.db.get_route_by_target(-2001)["enabled"])
        self.assertIsNone(self.db.claim_next_delivery(route["id"], eligible))
        self.db.mark_delivery(claimed["id"], "sent", target_message_ids=[900, 901])
        self.assertTrue(self.db.delete_route(-2001)["deleted"])
        self.assertIsNone(self.db.get_route_by_target(-2001))

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

    def test_topics_in_same_group_are_independent_targets(self) -> None:
        first = self.db.add_route(-1001, -2001, "话题群", 135, "动作电影")
        second = self.db.add_route(-1001, -2001, "话题群", 246, "喜剧电影")
        self.assertNotEqual(first, second)
        self.db.add_source(-1002, "另一个源", 0)
        with self.assertRaisesRegex(ValueError, "另一个源频道"):
            self.db.add_route(-1002, -2001, "话题群", 135, "动作电影")

        self.assertTrue(self.db.set_threshold(-2001, 7, 135))
        self.assertTrue(self.db.set_threshold(-2001, 9, 246))
        self.db.add_schedule(-2001, "09:00", date(2026, 7, 14), 135)
        self.db.add_schedule(-2001, "18:00", date(2026, 7, 14), 246)
        self.assertEqual(
            [row["time_hhmm"] for row in self.db.list_schedules(target_id=-2001, target_topic_id=135)],
            ["09:00"],
        )
        self.assertEqual(self.db.inventory(-2001, 135)[0]["threshold"], 7)
        self.assertEqual(self.db.inventory(-2001, 246)[0]["threshold"], 9)

    def test_routes_have_independent_history_timelines(self) -> None:
        self.db.add_route(
            -1001, -2003, "早期时间线", delivery_start_message_id=101
        )
        self.db.add_route(
            -1001, -2004, "较新时间线", delivery_start_message_id=101
        )
        self.db.backfill_route(
            -2003, start_message_id=1, target_message_id=100
        )
        self.db.backfill_route(
            -2004, start_message_id=50, target_message_id=100
        )
        self.db.add_schedule(-2003, "09:00", date(2026, 7, 14))
        self.db.add_schedule(-2004, "10:00", date(2026, 7, 14))

        self.ingest(600, [10, 11])
        self.ingest(601, [90, 91])
        self.ingest(602, [110, 111])
        self.assertEqual(self.db.get_route_by_target(-2003)["backfill_status"], "pending")
        self.assertEqual(self.db.get_route_by_target(-2004)["backfill_status"], "pending")

        generation = self.db.get_source(-1001)["scan_generation"]
        self.assertTrue(self.db.checkpoint_history(-1001, 50, generation))
        completed = self.db.complete_ready_backfills(self.db.get_source(-1001)["id"])
        self.assertEqual([row["target_telegram_id"] for row in completed], [-2004])
        self.assertEqual(self.db.get_route_by_target(-2003)["backfill_status"], "pending")
        self.assertEqual(self.db.get_route_by_target(-2004)["backfill_status"], "idle")
        self.assertEqual(self.db.delivery_counts(-2004), {"pending": 2})
        self.assertEqual(
            [row["target_telegram_id"] for row in self.db.list_schedules(enabled_only=True)],
            [-2004],
        )

        self.assertTrue(self.db.checkpoint_history(-1001, 1, generation))
        completed = self.db.complete_ready_backfills(self.db.get_source(-1001)["id"])
        self.assertEqual([row["target_telegram_id"] for row in completed], [-2003])
        self.assertEqual(self.db.delivery_counts(-2003), {"pending": 3})
        with self.assertRaisesRegex(ValueError, "只能向更早移动"):
            self.db.backfill_route(
                -2004, start_message_id=80, target_message_id=100
            )

    def test_new_history_request_invalidates_old_scan_generation(self) -> None:
        self.db.add_source(-1001, "源频道", 100, history_start_id=50)
        first = self.db.get_source(-1001)
        old_generation = int(first["scan_generation"])
        self.db.add_source(-1001, "源频道", 120, history_start_id=1)
        refreshed = self.db.get_source(-1001)
        self.assertGreater(refreshed["scan_generation"], old_generation)
        self.assertEqual(refreshed["history_snapshot_max_id"], 120)
        self.assertEqual(refreshed["history_covered_from_id"], 121)
        self.assertFalse(self.db.checkpoint_history(-1001, 40, old_generation))

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


class DatabaseMigrationTests(unittest.TestCase):
    def test_v1_routes_are_migrated_without_changing_ids_or_links(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "v1.sqlite3"
            conn = sqlite3.connect(path)
            conn.executescript(
                """
                CREATE TABLE schema_version(version INTEGER NOT NULL);
                INSERT INTO schema_version VALUES (1);
                CREATE TABLE sources (
                    id INTEGER PRIMARY KEY, telegram_id INTEGER NOT NULL UNIQUE,
                    title TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                    last_message_id INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL
                );
                CREATE TABLE routes (
                    id INTEGER PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    target_telegram_id INTEGER NOT NULL UNIQUE,
                    target_title TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                    last_sent_at TEXT, created_at TEXT NOT NULL
                );
                CREATE TABLE schedule_slots (
                    id INTEGER PRIMARY KEY,
                    route_id INTEGER NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
                    time_hhmm TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(route_id, time_hhmm)
                );
                CREATE TABLE schedule_state (
                    slot_id INTEGER PRIMARY KEY REFERENCES schedule_slots(id) ON DELETE CASCADE,
                    last_processed_date TEXT NOT NULL
                );
                CREATE TABLE alert_settings (
                    route_id INTEGER PRIMARY KEY REFERENCES routes(id) ON DELETE CASCADE,
                    threshold INTEGER NOT NULL DEFAULT 24 CHECK(threshold > 0),
                    enabled INTEGER NOT NULL DEFAULT 1, last_alert_date TEXT
                );
                CREATE TABLE service_settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                INSERT INTO sources VALUES (7, -1001, '源频道', 1, 99, '2026-07-15T00:00:00Z');
                INSERT INTO routes VALUES (11, 7, -2001, '旧目标', 1, NULL, '2026-07-15T00:00:00Z');
                INSERT INTO schedule_slots VALUES (13, 11, '09:00', 1);
                INSERT INTO schedule_state VALUES (13, '2026-07-14');
                INSERT INTO alert_settings VALUES (11, 30, 1, NULL);
                """
            )
            conn.close()

            db = Database(path)
            try:
                db.initialize()
                route = db.get_route_by_target(-2001)
                self.assertEqual(route["id"], 11)
                self.assertEqual(route["target_topic_id"], 0)
                self.assertIsNone(route["target_topic_title"])
                self.assertEqual(db.list_schedules(target_id=-2001)[0]["id"], 13)
                self.assertEqual(db.inventory(-2001)[0]["threshold"], 30)
                self.assertEqual(route["delivery_start_message_id"], 100)
                self.assertEqual(db.conn.execute("SELECT version FROM schema_version").fetchone()[0], 3)
                self.assertEqual(db.conn.execute("PRAGMA foreign_key_check").fetchall(), [])
            finally:
                db.close()

    def test_v2_album_ranges_and_route_start_are_migrated_to_v3(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            path = Path(tempdir) / "v2.sqlite3"
            conn = sqlite3.connect(path)
            conn.executescript(
                """
                CREATE TABLE schema_version(version INTEGER NOT NULL);
                INSERT INTO schema_version VALUES (2);
                CREATE TABLE sources (
                    id INTEGER PRIMARY KEY, telegram_id INTEGER NOT NULL UNIQUE,
                    title TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1,
                    last_message_id INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL
                );
                CREATE TABLE routes (
                    id INTEGER PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    target_telegram_id INTEGER NOT NULL,
                    target_topic_id INTEGER NOT NULL DEFAULT 0,
                    target_title TEXT NOT NULL, target_topic_title TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1, last_sent_at TEXT, created_at TEXT NOT NULL,
                    UNIQUE(target_telegram_id, target_topic_id)
                );
                CREATE TABLE source_albums (
                    id INTEGER PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    grouped_id TEXT NOT NULL, message_ids_json TEXT NOT NULL,
                    source_date TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(source_id, grouped_id)
                );
                INSERT INTO sources VALUES (1, -1001, '源', 1, 500, '2026-07-15T00:00:00Z');
                INSERT INTO routes VALUES (2, 1, -2001, 0, '目标', NULL, 1, NULL, '2026-07-15T00:00:00Z');
                INSERT INTO source_albums VALUES (
                    3, 1, '700', '[420, 418, 419]', '2026-07-15T00:00:00Z',
                    'active', '2026-07-15T00:00:00Z', '2026-07-15T00:00:00Z'
                );
                """
            )
            conn.close()
            db = Database(path)
            try:
                db.initialize()
                route = db.get_route_by_target(-2001)
                album = db.conn.execute("SELECT * FROM source_albums WHERE id = 3").fetchone()
                self.assertEqual(route["delivery_start_message_id"], 501)
                self.assertEqual((album["first_message_id"], album["last_message_id"]), (418, 420))
                self.assertEqual(db.conn.execute("SELECT version FROM schema_version").fetchone()[0], 3)
            finally:
                db.close()
