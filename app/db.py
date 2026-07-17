from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any, Iterator

from .timeutils import to_utc_iso, utc_now


SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    telegram_id INTEGER NOT NULL UNIQUE,
    title TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_message_id INTEGER NOT NULL DEFAULT 0,
    forward_scan_status TEXT NOT NULL DEFAULT 'idle'
        CHECK(forward_scan_status IN ('idle', 'scanning', 'failed')),
    forward_scan_error TEXT,
    history_requested_min_id INTEGER,
    history_covered_from_id INTEGER,
    history_snapshot_max_id INTEGER,
    history_scan_status TEXT NOT NULL DEFAULT 'idle'
        CHECK(history_scan_status IN ('idle', 'pending', 'scanning', 'failed')),
    history_scan_error TEXT,
    history_requested_by INTEGER,
    scan_generation INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    target_telegram_id INTEGER NOT NULL,
    target_topic_id INTEGER NOT NULL DEFAULT 0 CHECK(target_topic_id >= 0),
    target_title TEXT NOT NULL,
    target_topic_title TEXT,
    delivery_start_message_id INTEGER NOT NULL DEFAULT 1 CHECK(delivery_start_message_id > 0),
    backfill_target_message_id INTEGER,
    backfill_status TEXT NOT NULL DEFAULT 'idle'
        CHECK(backfill_status IN ('idle', 'pending', 'failed')),
    backfill_error TEXT,
    backfill_requested_by INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_sent_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(target_telegram_id, target_topic_id)
);

CREATE TABLE IF NOT EXISTS schedule_slots (
    id INTEGER PRIMARY KEY,
    route_id INTEGER NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    time_hhmm TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    UNIQUE(route_id, time_hhmm)
);

CREATE TABLE IF NOT EXISTS schedule_state (
    slot_id INTEGER PRIMARY KEY REFERENCES schedule_slots(id) ON DELETE CASCADE,
    last_processed_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_albums (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    grouped_id TEXT NOT NULL,
    message_ids_json TEXT NOT NULL,
    first_message_id INTEGER NOT NULL,
    last_message_id INTEGER NOT NULL,
    source_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'cancelled')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(source_id, grouped_id)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY,
    album_id INTEGER NOT NULL REFERENCES source_albums(id) ON DELETE CASCADE,
    route_id INTEGER NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'sending', 'sent', 'failed', 'cancelled', 'ambiguous')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    target_message_ids_json TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(album_id, route_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_queue
ON deliveries(route_id, status, id);

CREATE TABLE IF NOT EXISTS alert_settings (
    route_id INTEGER PRIMARY KEY REFERENCES routes(id) ON DELETE CASCADE,
    threshold INTEGER NOT NULL DEFAULT 24 CHECK(threshold > 0),
    enabled INTEGER NOT NULL DEFAULT 1,
    last_alert_date TEXT
);

CREATE TABLE IF NOT EXISTS service_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

CURRENT_SCHEMA_VERSION = 3


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA busy_timeout = 5000")

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                self.conn.execute("BEGIN IMMEDIATE")
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def initialize(self) -> None:
        with self._lock:
            self.conn.executescript(SCHEMA)
            version_row = self.conn.execute(
                "SELECT version FROM schema_version ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if version_row is None:
                self.conn.execute(
                    "INSERT INTO schema_version(version) VALUES (?)",
                    (CURRENT_SCHEMA_VERSION,),
                )
            else:
                version = int(version_row["version"])
                if version < 1 or version > CURRENT_SCHEMA_VERSION:
                    raise RuntimeError(f"不支持的数据库版本: {version}")
                self.conn.commit()
                if version == 1:
                    self._migrate_v1_to_v2()
                    version = 2
                if version == 2:
                    self._migrate_v2_to_v3()
            self.conn.execute(
                "INSERT OR IGNORE INTO service_settings(key, value) VALUES ('timezone', 'Asia/Shanghai')"
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO service_settings(key, value) VALUES ('alert_time', '09:00')"
            )
            self.conn.commit()

    def _migrate_v1_to_v2(self) -> None:
        """Rebuild routes so existing route IDs and all foreign-key links are preserved."""
        self.conn.execute("PRAGMA foreign_keys = OFF")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self.conn.execute(
                """
                CREATE TABLE routes_v2 (
                    id INTEGER PRIMARY KEY,
                    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
                    target_telegram_id INTEGER NOT NULL,
                    target_topic_id INTEGER NOT NULL DEFAULT 0 CHECK(target_topic_id >= 0),
                    target_title TEXT NOT NULL,
                    target_topic_title TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_sent_at TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(target_telegram_id, target_topic_id)
                )
                """
            )
            self.conn.execute(
                """
                INSERT INTO routes_v2(
                    id, source_id, target_telegram_id, target_topic_id,
                    target_title, target_topic_title, enabled, last_sent_at, created_at
                )
                SELECT id, source_id, target_telegram_id, 0,
                       target_title, NULL, enabled, last_sent_at, created_at
                FROM routes
                """
            )
            self.conn.execute("DROP TABLE routes")
            self.conn.execute("ALTER TABLE routes_v2 RENAME TO routes")
            self.conn.execute("UPDATE schema_version SET version = 2")
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.execute("PRAGMA foreign_keys = ON")
        violations = self.conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            raise RuntimeError(f"数据库迁移后外键检查失败: {violations}")

    def _migrate_v2_to_v3(self) -> None:
        with self.transaction() as conn:
            def add_missing_column(table: str, definition: str) -> None:
                name = definition.split(None, 1)[0]
                existing = {
                    str(row["name"])
                    for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
                }
                if name not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")

            source_columns = (
                "forward_scan_status TEXT NOT NULL DEFAULT 'idle'",
                "forward_scan_error TEXT",
                "history_requested_min_id INTEGER",
                "history_covered_from_id INTEGER",
                "history_snapshot_max_id INTEGER",
                "history_scan_status TEXT NOT NULL DEFAULT 'idle'",
                "history_scan_error TEXT",
                "history_requested_by INTEGER",
                "scan_generation INTEGER NOT NULL DEFAULT 0",
            )
            for definition in source_columns:
                add_missing_column("sources", definition)

            route_columns = (
                "delivery_start_message_id INTEGER NOT NULL DEFAULT 1",
                "backfill_target_message_id INTEGER",
                "backfill_status TEXT NOT NULL DEFAULT 'idle'",
                "backfill_error TEXT",
                "backfill_requested_by INTEGER",
            )
            for definition in route_columns:
                add_missing_column("routes", definition)

            add_missing_column(
                "source_albums", "first_message_id INTEGER NOT NULL DEFAULT 0"
            )
            add_missing_column(
                "source_albums", "last_message_id INTEGER NOT NULL DEFAULT 0"
            )
            for album in conn.execute(
                "SELECT id, message_ids_json FROM source_albums"
            ).fetchall():
                message_ids = [int(value) for value in json.loads(album["message_ids_json"])]
                conn.execute(
                    "UPDATE source_albums SET first_message_id = ?, last_message_id = ? WHERE id = ?",
                    (min(message_ids), max(message_ids), album["id"]),
                )
            conn.execute(
                """
                UPDATE sources
                SET history_snapshot_max_id = last_message_id,
                    history_covered_from_id = last_message_id + 1
                """
            )
            conn.execute(
                """
                UPDATE routes
                SET delivery_start_message_id = (
                    SELECT s.last_message_id + 1 FROM sources s WHERE s.id = routes.source_id
                )
                """
            )
            conn.execute("UPDATE schema_version SET version = 3")

    def add_source(
        self,
        telegram_id: int,
        title: str,
        watermark: int,
        *,
        history_start_id: int | None = None,
        requested_by: int | None = None,
    ) -> int:
        if history_start_id is not None and history_start_id <= 0:
            raise ValueError("历史起点消息 ID 必须大于 0")
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM sources WHERE telegram_id = ?", (telegram_id,)
            ).fetchone()
            if existing:
                if history_start_id is None:
                    conn.execute(
                        "UPDATE sources SET title = ?, enabled = 1 WHERE id = ?",
                        (title, existing["id"]),
                    )
                else:
                    current_requested = existing["history_requested_min_id"]
                    requested = (
                        min(int(current_requested), history_start_id)
                        if current_requested is not None
                        else history_start_id
                    )
                    # An explicit history request takes a fresh Telegram snapshot.
                    # The generation prevents an older in-flight scan from moving this cursor.
                    snapshot = watermark
                    covered = watermark + 1
                    status = "pending" if int(covered) > requested else "idle"
                    conn.execute(
                        """
                        UPDATE sources
                        SET title = ?, enabled = 1,
                            history_requested_min_id = ?,
                            history_snapshot_max_id = ?,
                            history_covered_from_id = ?,
                            history_scan_status = ?, history_scan_error = NULL,
                            history_requested_by = ?,
                            scan_generation = scan_generation + 1
                        WHERE id = ?
                        """,
                        (
                            title,
                            requested,
                            snapshot,
                            covered,
                            status,
                            requested_by,
                            existing["id"],
                        ),
                    )
                return int(existing["id"])
            cursor = conn.execute(
                """
                INSERT INTO sources(
                    telegram_id, title, last_message_id,
                    history_requested_min_id, history_covered_from_id,
                    history_snapshot_max_id, history_scan_status,
                    history_requested_by, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    title,
                    watermark,
                    history_start_id,
                    watermark + 1,
                    watermark,
                    "pending" if history_start_id is not None and history_start_id <= watermark else "idle",
                    requested_by,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def list_sources(self, enabled_only: bool = False) -> list[sqlite3.Row]:
        query = "SELECT * FROM sources"
        params: tuple[Any, ...] = ()
        if enabled_only:
            query += " WHERE enabled = 1"
        query += " ORDER BY id"
        with self._lock:
            return list(self.conn.execute(query, params).fetchall())

    def get_source(self, telegram_id: int, enabled_only: bool = False) -> sqlite3.Row | None:
        query = "SELECT * FROM sources WHERE telegram_id = ?"
        if enabled_only:
            query += " AND enabled = 1"
        with self._lock:
            return self.conn.execute(query, (telegram_id,)).fetchone()

    def set_source_enabled(self, telegram_id: int, enabled: bool) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE sources SET enabled = ? WHERE telegram_id = ?",
                (int(enabled), telegram_id),
            )
            return cursor.rowcount > 0

    def update_watermark(self, telegram_id: int, message_id: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE sources SET last_message_id = MAX(last_message_id, ?) WHERE telegram_id = ?",
                (message_id, telegram_id),
            )

    def set_forward_scan_state(
        self, telegram_id: int, status: str, error: str | None = None
    ) -> None:
        if status not in {"idle", "scanning", "failed"}:
            raise ValueError("无效正向扫描状态")
        with self.transaction() as conn:
            conn.execute(
                "UPDATE sources SET forward_scan_status = ?, forward_scan_error = ? WHERE telegram_id = ?",
                (status, error, telegram_id),
            )

    def list_history_sources(self) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self.conn.execute(
                    """
                    SELECT * FROM sources
                    WHERE enabled = 1
                      AND history_requested_min_id IS NOT NULL
                      AND history_covered_from_id > history_requested_min_id
                    ORDER BY history_covered_from_id DESC, id
                    """
                ).fetchall()
            )

    def begin_history_scan(self, telegram_id: int) -> sqlite3.Row | None:
        with self.transaction() as conn:
            updated = conn.execute(
                """
                UPDATE sources
                SET history_scan_status = 'scanning', history_scan_error = NULL
                WHERE telegram_id = ? AND enabled = 1
                  AND history_requested_min_id IS NOT NULL
                  AND history_covered_from_id > history_requested_min_id
                """,
                (telegram_id,),
            )
            if not updated.rowcount:
                return None
            source = conn.execute(
                "SELECT * FROM sources WHERE telegram_id = ?", (telegram_id,)
            ).fetchone()
            if source:
                conn.execute(
                    """
                    UPDATE routes
                    SET backfill_status = 'pending', backfill_error = NULL
                    WHERE source_id = ? AND backfill_status = 'failed'
                    """,
                    (source["id"],),
                )
            return source

    def lower_history_request(self, source_id: int, start_id: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE sources
                SET history_requested_min_id = CASE
                        WHEN history_requested_min_id IS NULL THEN ?
                        ELSE MIN(history_requested_min_id, ?)
                    END,
                    history_scan_status = CASE
                        WHEN history_covered_from_id > ? THEN 'pending'
                        ELSE history_scan_status
                    END,
                    history_scan_error = NULL
                WHERE id = ?
                """,
                (start_id, start_id, start_id, source_id),
            )

    def checkpoint_history(
        self, telegram_id: int, covered_from_id: int, generation: int
    ) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE sources
                SET history_covered_from_id = MIN(history_covered_from_id, ?)
                WHERE telegram_id = ? AND enabled = 1 AND scan_generation = ?
                """,
                (covered_from_id, telegram_id, generation),
            )
            return cursor.rowcount > 0

    def finish_history_scan(self, telegram_id: int, generation: int) -> int | None:
        with self.transaction() as conn:
            source = conn.execute(
                "SELECT * FROM sources WHERE telegram_id = ?", (telegram_id,)
            ).fetchone()
            if not source or int(source["scan_generation"]) != generation:
                return None
            complete = (
                source["history_requested_min_id"] is None
                or int(source["history_covered_from_id"])
                <= int(source["history_requested_min_id"])
            )
            conn.execute(
                """
                UPDATE sources
                SET history_scan_status = ?, history_scan_error = NULL,
                    history_requested_by = CASE WHEN ? THEN NULL ELSE history_requested_by END
                WHERE id = ?
                """,
                ("idle" if complete else "pending", int(complete), source["id"]),
            )
            return int(source["history_requested_by"]) if complete and source["history_requested_by"] else None

    def fail_history_scan(
        self, telegram_id: int, error: str
    ) -> tuple[int | None, list[sqlite3.Row]]:
        with self.transaction() as conn:
            source = conn.execute(
                "SELECT id, history_requested_by FROM sources WHERE telegram_id = ?",
                (telegram_id,),
            ).fetchone()
            if not source:
                return None, []
            affected_routes = list(
                conn.execute(
                    """
                    SELECT * FROM routes
                    WHERE source_id = ? AND backfill_status = 'pending'
                      AND backfill_requested_by IS NOT NULL
                    """,
                    (source["id"],),
                ).fetchall()
            )
            conn.execute(
                """
                UPDATE sources
                SET history_scan_status = 'failed', history_scan_error = ?,
                    history_requested_by = NULL
                WHERE id = ?
                """,
                (error[:1000], source["id"]),
            )
            conn.execute(
                """
                UPDATE routes
                SET backfill_status = 'failed', backfill_error = ?,
                    backfill_requested_by = NULL
                WHERE source_id = ? AND backfill_status = 'pending'
                """,
                (error[:1000], source["id"]),
            )
            requester = (
                int(source["history_requested_by"])
                if source["history_requested_by"]
                else None
            )
            return requester, affected_routes

    def add_route(
        self,
        source_telegram_id: int,
        target_id: int,
        target_title: str,
        target_topic_id: int = 0,
        target_topic_title: str | None = None,
        delivery_start_message_id: int = 1,
    ) -> int:
        if target_topic_id < 0:
            raise ValueError("话题 ID 必须是非负整数")
        if delivery_start_message_id <= 0:
            raise ValueError("路由起点消息 ID 必须大于 0")
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            source = conn.execute(
                "SELECT id FROM sources WHERE telegram_id = ?", (source_telegram_id,)
            ).fetchone()
            if not source:
                raise ValueError("源频道尚未添加")
            existing = conn.execute(
                "SELECT id, source_id FROM routes WHERE target_telegram_id = ? AND target_topic_id = ?",
                (target_id, target_topic_id),
            ).fetchone()
            if existing:
                if existing["source_id"] != source["id"]:
                    raise ValueError("该目标频道已经映射到另一个源频道")
                conn.execute(
                    "UPDATE routes SET target_title = ?, target_topic_title = ?, enabled = 1 WHERE id = ?",
                    (target_title, target_topic_title, existing["id"]),
                )
                return int(existing["id"])
            cursor = conn.execute(
                """
                INSERT INTO routes(
                    source_id, target_telegram_id, target_topic_id,
                    target_title, target_topic_title,
                    delivery_start_message_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source["id"], target_id, target_topic_id, target_title,
                    target_topic_title, delivery_start_message_id, now,
                ),
            )
            route_id = int(cursor.lastrowid)
            conn.execute("INSERT INTO alert_settings(route_id) VALUES (?)", (route_id,))
            return route_id

    def list_routes(self, enabled_only: bool = False) -> list[sqlite3.Row]:
        query = """
            SELECT r.*, s.telegram_id AS source_telegram_id, s.title AS source_title,
                   a.threshold, a.enabled AS alert_enabled, a.last_alert_date
            FROM routes r
            JOIN sources s ON s.id = r.source_id
            JOIN alert_settings a ON a.route_id = r.id
        """
        if enabled_only:
            query += " WHERE r.enabled = 1 AND s.enabled = 1"
        query += " ORDER BY r.id"
        with self._lock:
            return list(self.conn.execute(query).fetchall())

    def get_route_by_target(
        self, target_id: int, target_topic_id: int = 0
    ) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                """
                SELECT r.*, s.telegram_id AS source_telegram_id, s.title AS source_title
                FROM routes r JOIN sources s ON s.id = r.source_id
                WHERE r.target_telegram_id = ? AND r.target_topic_id = ?
                """,
                (target_id, target_topic_id),
            ).fetchone()

    def set_route_enabled(
        self, target_id: int, enabled: bool, target_topic_id: int = 0
    ) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE routes SET enabled = ? WHERE target_telegram_id = ? AND target_topic_id = ?",
                (int(enabled), target_id, target_topic_id),
            )
            return cursor.rowcount > 0

    def delete_route(
        self, target_id: int, target_topic_id: int = 0
    ) -> dict[str, Any] | None:
        """Hard-delete one target route, blocking safely while a send is active."""
        with self.transaction() as conn:
            route = conn.execute(
                """
                SELECT id, target_telegram_id, target_topic_id,
                       target_title, target_topic_title
                FROM routes
                WHERE target_telegram_id = ? AND target_topic_id = ?
                """,
                (target_id, target_topic_id),
            ).fetchone()
            if not route:
                return None

            route_id = int(route["id"])
            # Disable first inside the same write transaction. Together with the
            # enabled checks in claim_next_delivery, this closes the claim/delete race.
            conn.execute("UPDATE routes SET enabled = 0 WHERE id = ?", (route_id,))
            status_counts = {
                str(row["status"]): int(row["count"])
                for row in conn.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM deliveries WHERE route_id = ? GROUP BY status
                    """,
                    (route_id,),
                ).fetchall()
            }
            schedule_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM schedule_slots WHERE route_id = ?",
                    (route_id,),
                ).fetchone()[0]
            )
            result: dict[str, Any] = {
                "deleted": False,
                "target_telegram_id": int(route["target_telegram_id"]),
                "target_topic_id": int(route["target_topic_id"]),
                "target_title": str(route["target_title"]),
                "target_topic_title": route["target_topic_title"],
                "delivery_counts": status_counts,
                "schedule_count": schedule_count,
            }
            if status_counts.get("sending", 0):
                return result

            conn.execute("DELETE FROM routes WHERE id = ?", (route_id,))
            result["deleted"] = True
            return result

    def add_schedule(
        self, target_id: int, hhmm: str, initial_date: date, target_topic_id: int = 0
    ) -> int:
        return self.add_schedules(
            target_id, [(hhmm, initial_date)], target_topic_id=target_topic_id
        )[0][0]

    def add_schedules(
        self,
        target_id: int,
        schedules: list[tuple[str, date]],
        target_topic_id: int = 0,
    ) -> list[tuple[int, str, bool]]:
        """Atomically add schedules, returning (slot_id, hhmm, created)."""
        if not schedules:
            raise ValueError("至少需要一个发布时间")
        with self.transaction() as conn:
            route = conn.execute(
                "SELECT id FROM routes WHERE target_telegram_id = ? AND target_topic_id = ?",
                (target_id, target_topic_id),
            ).fetchone()
            if not route:
                raise ValueError("目标频道尚未映射")
            results: list[tuple[int, str, bool]] = []
            for hhmm, initial_date in schedules:
                existing = conn.execute(
                    "SELECT id FROM schedule_slots WHERE route_id = ? AND time_hhmm = ?",
                    (route["id"], hhmm),
                ).fetchone()
                if existing:
                    conn.execute(
                        "UPDATE schedule_slots SET enabled = 1 WHERE id = ?", (existing["id"],)
                    )
                    results.append((int(existing["id"]), hhmm, False))
                    continue
                cursor = conn.execute(
                    "INSERT INTO schedule_slots(route_id, time_hhmm) VALUES (?, ?)",
                    (route["id"], hhmm),
                )
                slot_id = int(cursor.lastrowid)
                conn.execute(
                    "INSERT INTO schedule_state(slot_id, last_processed_date) VALUES (?, ?)",
                    (slot_id, initial_date.isoformat()),
                )
                results.append((slot_id, hhmm, True))
            return results

    def delete_schedule(self, slot_id: int) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute("DELETE FROM schedule_slots WHERE id = ?", (slot_id,))
            return cursor.rowcount > 0

    def delete_schedules_by_target(
        self, target_id: int, times: list[str] | None, target_topic_id: int = 0
    ) -> tuple[list[str], list[str]]:
        """Delete all target schedules when times is None, otherwise selected times."""
        with self.transaction() as conn:
            route = conn.execute(
                "SELECT id FROM routes WHERE target_telegram_id = ? AND target_topic_id = ?",
                (target_id, target_topic_id),
            ).fetchone()
            if not route:
                raise ValueError("目标频道尚未映射")
            existing_rows = conn.execute(
                "SELECT id, time_hhmm FROM schedule_slots WHERE route_id = ? ORDER BY time_hhmm",
                (route["id"],),
            ).fetchall()
            existing = {str(row["time_hhmm"]): int(row["id"]) for row in existing_rows}
            if times is None:
                selected = list(existing)
                missing: list[str] = []
            else:
                selected = [hhmm for hhmm in times if hhmm in existing]
                missing = [hhmm for hhmm in times if hhmm not in existing]
            if selected:
                placeholders = ",".join("?" for _ in selected)
                conn.execute(
                    f"DELETE FROM schedule_slots WHERE route_id = ? AND time_hhmm IN ({placeholders})",
                    (route["id"], *selected),
                )
            return selected, missing

    def list_schedules(
        self,
        enabled_only: bool = False,
        target_id: int | None = None,
        target_topic_id: int = 0,
    ) -> list[sqlite3.Row]:
        query = """
            SELECT sl.id, sl.route_id, sl.time_hhmm, sl.enabled, st.last_processed_date,
                   r.target_telegram_id, r.target_topic_id,
                   r.target_title, r.target_topic_title
            FROM schedule_slots sl
            JOIN schedule_state st ON st.slot_id = sl.id
            JOIN routes r ON r.id = sl.route_id
            JOIN sources s ON s.id = r.source_id
        """
        conditions: list[str] = []
        params: list[Any] = []
        if enabled_only:
            conditions.append(
                "sl.enabled = 1 AND r.enabled = 1 AND s.enabled = 1 "
                "AND r.backfill_status = 'idle' AND s.forward_scan_status = 'idle'"
            )
        if target_id is not None:
            conditions.append("r.target_telegram_id = ? AND r.target_topic_id = ?")
            params.extend((target_id, target_topic_id))
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY r.target_telegram_id, r.target_topic_id, sl.time_hhmm, sl.id"
        with self._lock:
            return list(self.conn.execute(query, tuple(params)).fetchall())

    def backfill_route(
        self,
        target_id: int,
        target_topic_id: int = 0,
        *,
        start_message_id: int = 1,
        target_message_id: int | None = None,
        requested_by: int | None = None,
    ) -> tuple[int, int, int]:
        """Move a route's start earlier and fill every currently known eligible album."""
        if start_message_id <= 0:
            raise ValueError("回填起点消息 ID 必须大于 0")
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            route = conn.execute(
                """
                SELECT r.*, s.last_message_id AS source_watermark,
                       s.history_snapshot_max_id, s.history_covered_from_id
                FROM routes r JOIN sources s ON s.id = r.source_id
                WHERE target_telegram_id = ? AND target_topic_id = ?
                """,
                (target_id, target_topic_id),
            ).fetchone()
            if not route:
                raise ValueError("目标频道尚未映射")
            current_start = int(route["delivery_start_message_id"])
            if start_message_id > current_start:
                raise ValueError(
                    f"路由起点只能向更早移动，当前起点为 {current_start}"
                )
            effective_start = min(current_start, start_message_id)
            target_message_id = (
                int(target_message_id)
                if target_message_id is not None
                else int(route["source_watermark"])
            )
            snapshot = int(route["history_snapshot_max_id"] or 0)
            covered = int(route["history_covered_from_id"] or (snapshot + 1))
            history_ready = effective_start > snapshot or covered <= effective_start
            forward_ready = int(route["source_watermark"]) >= target_message_id
            ready = history_ready and forward_ready
            conn.execute(
                """
                UPDATE routes
                SET delivery_start_message_id = ?, backfill_target_message_id = ?,
                    backfill_status = ?, backfill_error = NULL,
                    backfill_requested_by = ?
                WHERE id = ?
                """,
                (
                    effective_start,
                    target_message_id,
                    "idle" if ready else "pending",
                    None if ready else requested_by,
                    route["id"],
                ),
            )
            if not history_ready:
                conn.execute(
                    """
                    UPDATE sources
                    SET history_requested_min_id = CASE
                            WHEN history_requested_min_id IS NULL THEN ?
                            ELSE MIN(history_requested_min_id, ?)
                        END,
                        history_scan_status = 'pending', history_scan_error = NULL
                    WHERE id = ?
                    """,
                    (effective_start, effective_start, route["source_id"]),
                )
            album_rows = conn.execute(
                """
                SELECT id FROM source_albums
                WHERE source_id = ? AND status = 'active'
                  AND last_message_id >= ?
                ORDER BY source_date, id
                """,
                (route["source_id"], effective_start),
            ).fetchall()
            album_ids = [int(row["id"]) for row in album_rows]
            existing_rows = conn.execute(
                "SELECT album_id FROM deliveries WHERE route_id = ?", (route["id"],)
            ).fetchall()
            existing_ids = {int(row["album_id"]) for row in existing_rows}
            missing_ids = [album_id for album_id in album_ids if album_id not in existing_ids]
            conn.executemany(
                """
                INSERT INTO deliveries(album_id, route_id, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                [(album_id, route["id"], now, now) for album_id in missing_ids],
            )
            pending = conn.execute(
                "SELECT COUNT(*) FROM deliveries WHERE route_id = ? AND status = 'pending'",
                (route["id"],),
            ).fetchone()[0]
            return len(missing_ids), len(album_ids) - len(missing_ids), int(pending)

    def complete_ready_backfills(self, source_id: int) -> list[sqlite3.Row]:
        now = to_utc_iso(utc_now())
        completed_ids: list[int] = []
        with self.transaction() as conn:
            source = conn.execute("SELECT * FROM sources WHERE id = ?", (source_id,)).fetchone()
            if not source:
                return []
            routes = conn.execute(
                "SELECT * FROM routes WHERE source_id = ? AND backfill_status = 'pending'",
                (source_id,),
            ).fetchall()
            for route in routes:
                start_id = int(route["delivery_start_message_id"])
                snapshot = int(source["history_snapshot_max_id"] or 0)
                covered = int(source["history_covered_from_id"] or (snapshot + 1))
                target = int(route["backfill_target_message_id"] or 0)
                if not (start_id > snapshot or covered <= start_id):
                    continue
                if int(source["last_message_id"]) < target:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO deliveries(album_id, route_id, created_at, updated_at)
                    SELECT a.id, ?, ?, ? FROM source_albums a
                    WHERE a.source_id = ? AND a.status = 'active'
                      AND a.last_message_id >= ?
                    """,
                    (route["id"], now, now, source_id, start_id),
                )
                conn.execute(
                    "UPDATE routes SET backfill_status = 'idle', backfill_error = NULL WHERE id = ?",
                    (route["id"],),
                )
                completed_ids.append(int(route["id"]))
            if not completed_ids:
                return []
            placeholders = ",".join("?" for _ in completed_ids)
            rows = conn.execute(
                f"""
                SELECT r.*, s.telegram_id AS source_telegram_id, s.title AS source_title
                FROM routes r JOIN sources s ON s.id = r.source_id
                WHERE r.id IN ({placeholders})
                """,
                tuple(completed_ids),
            ).fetchall()
            conn.executemany(
                "UPDATE routes SET backfill_requested_by = NULL WHERE id = ?",
                [(route_id,) for route_id in completed_ids],
            )
            return list(rows)

    def mark_schedule_processed(self, slot_id: int, processed_date: date) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE schedule_state SET last_processed_date = ? WHERE slot_id = ?",
                (processed_date.isoformat(), slot_id),
            )

    def ingest_album(
        self,
        source_telegram_id: int,
        grouped_id: str | int,
        message_ids: list[int],
        source_date: datetime,
    ) -> tuple[int, bool]:
        ids = sorted(set(int(value) for value in message_ids))
        if len(ids) < 2:
            raise ValueError("媒体组至少需要两条消息")
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            source = conn.execute(
                "SELECT id, enabled FROM sources WHERE telegram_id = ?", (source_telegram_id,)
            ).fetchone()
            if not source or not source["enabled"]:
                raise ValueError("源频道不存在或未启用")
            existing = conn.execute(
                "SELECT id, status FROM source_albums WHERE source_id = ? AND grouped_id = ?",
                (source["id"], str(grouped_id)),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE source_albums
                    SET message_ids_json = ?, first_message_id = ?, last_message_id = ?,
                        source_date = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        json.dumps(ids), min(ids), max(ids), to_utc_iso(source_date),
                        now, existing["id"],
                    ),
                )
                album_id = int(existing["id"])
                created = False
                album_active = existing["status"] == "active"
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO source_albums(
                        source_id, grouped_id, message_ids_json,
                        first_message_id, last_message_id,
                        source_date, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        source["id"], str(grouped_id), json.dumps(ids), min(ids), max(ids),
                        to_utc_iso(source_date), now, now,
                    ),
                )
                album_id = int(cursor.lastrowid)
                created = True
                album_active = True
            routes = conn.execute(
                """
                SELECT id FROM routes
                WHERE source_id = ? AND enabled = 1
                  AND delivery_start_message_id <= ?
                """,
                (source["id"], max(ids)),
            ).fetchall()
            if album_active:
                conn.executemany(
                    "INSERT OR IGNORE INTO deliveries(album_id, route_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
                    [(album_id, route["id"], now, now) for route in routes],
                )
            return album_id, created

    def cancel_albums_containing(self, source_telegram_id: int, deleted_ids: list[int]) -> int:
        deleted = set(deleted_ids)
        now = to_utc_iso(utc_now())
        count = 0
        with self.transaction() as conn:
            albums = conn.execute(
                """
                SELECT a.id, a.message_ids_json FROM source_albums a
                JOIN sources s ON s.id = a.source_id
                WHERE s.telegram_id = ? AND a.status = 'active'
                """,
                (source_telegram_id,),
            ).fetchall()
            for album in albums:
                if deleted.intersection(json.loads(album["message_ids_json"])):
                    conn.execute(
                        "UPDATE source_albums SET status = 'cancelled', updated_at = ? WHERE id = ?",
                        (now, album["id"]),
                    )
                    conn.execute(
                        "UPDATE deliveries SET status = 'cancelled', last_error = ?, updated_at = ? "
                        "WHERE album_id = ? AND status IN ('pending', 'sending', 'failed')",
                        ("源媒体组已删除或不完整", now, album["id"]),
                    )
                    count += 1
        return count

    def cancel_album(self, album_id: int, reason: str) -> None:
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            conn.execute(
                "UPDATE source_albums SET status = 'cancelled', updated_at = ? WHERE id = ?",
                (now, album_id),
            )
            conn.execute(
                """
                UPDATE deliveries SET status = 'cancelled', last_error = ?, updated_at = ?
                WHERE album_id = ? AND status IN ('pending', 'sending', 'failed')
                """,
                (reason, now, album_id),
            )

    def claim_next_delivery(self, route_id: int, eligible_before: datetime) -> sqlite3.Row | None:
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            row = conn.execute(
                """
                SELECT d.id FROM deliveries d
                JOIN source_albums a ON a.id = d.album_id
                JOIN routes r ON r.id = d.route_id
                JOIN sources s ON s.id = r.source_id
                WHERE d.route_id = ? AND d.status = 'pending' AND a.status = 'active'
                  AND r.enabled = 1 AND s.enabled = 1
                  AND r.backfill_status = 'idle' AND s.forward_scan_status = 'idle'
                  AND a.source_date <= ?
                ORDER BY a.source_date, a.id, d.id LIMIT 1
                """,
                (route_id, to_utc_iso(eligible_before)),
            ).fetchone()
            if not row:
                return None
            updated = conn.execute(
                "UPDATE deliveries SET status = 'sending', attempt_count = attempt_count + 1, "
                "updated_at = ? WHERE id = ? AND status = 'pending'",
                (now, row["id"]),
            )
            if not updated.rowcount:
                return None
            return conn.execute(
                """
                SELECT d.*, a.grouped_id, a.message_ids_json, a.source_date, a.status AS album_status,
                       s.telegram_id AS source_telegram_id, s.title AS source_title,
                       r.target_telegram_id, r.target_topic_id,
                       r.target_title, r.target_topic_title, r.last_sent_at
                FROM deliveries d
                JOIN source_albums a ON a.id = d.album_id
                JOIN sources s ON s.id = a.source_id
                JOIN routes r ON r.id = d.route_id
                WHERE d.id = ?
                """,
                (row["id"],),
            ).fetchone()

    def mark_delivery(
        self,
        delivery_id: int,
        status: str,
        *,
        target_message_ids: list[int] | None = None,
        error: str | None = None,
    ) -> None:
        if status not in {"pending", "sending", "sent", "failed", "cancelled", "ambiguous"}:
            raise ValueError("无效投递状态")
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            delivery = conn.execute(
                "SELECT route_id FROM deliveries WHERE id = ?", (delivery_id,)
            ).fetchone()
            if not delivery:
                raise ValueError("投递不存在")
            conn.execute(
                """
                UPDATE deliveries
                SET status = ?, target_message_ids_json = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    json.dumps(target_message_ids) if target_message_ids is not None else None,
                    error,
                    now,
                    delivery_id,
                ),
            )
            if status == "sent":
                conn.execute(
                    "UPDATE routes SET last_sent_at = ? WHERE id = ?",
                    (now, delivery["route_id"]),
                )

    def retry_delivery(self, delivery_id: int) -> bool:
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE deliveries SET status = 'pending', last_error = NULL, updated_at = ?
                WHERE id = ? AND status IN ('failed', 'ambiguous')
                """,
                (now, delivery_id),
            )
            return cursor.rowcount > 0

    def skip_delivery(self, delivery_id: int) -> bool:
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE deliveries SET status = 'cancelled', last_error = '管理员跳过', updated_at = ?
                WHERE id = ? AND status IN ('pending', 'failed', 'ambiguous')
                """,
                (now, delivery_id),
            )
            return cursor.rowcount > 0

    def recover_interrupted(self) -> int:
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE deliveries SET status = 'ambiguous',
                    last_error = '服务在发送确认前中断，请人工确认', updated_at = ?
                WHERE status = 'sending'
                """,
                (now,),
            )
            return cursor.rowcount

    def inventory(
        self, target_id: int | None = None, target_topic_id: int = 0
    ) -> list[sqlite3.Row]:
        query = """
            SELECT r.id AS route_id, r.target_telegram_id, r.target_topic_id,
                   r.target_title, r.target_topic_title,
                   r.enabled AS route_enabled, r.delivery_start_message_id,
                   r.backfill_target_message_id, r.backfill_status, r.backfill_error,
                   s.enabled AS source_enabled, s.forward_scan_status,
                   s.telegram_id AS source_telegram_id, s.title AS source_title,
                   a.threshold, a.enabled AS alert_enabled, a.last_alert_date,
                   SUM(CASE WHEN d.status = 'pending' AND sa.status = 'active' THEN 1 ELSE 0 END) AS pending_count,
                   (SELECT GROUP_CONCAT(sl.time_hhmm, ',') FROM schedule_slots sl
                    WHERE sl.route_id = r.id AND sl.enabled = 1) AS schedule_times
            FROM routes r
            JOIN sources s ON s.id = r.source_id
            JOIN alert_settings a ON a.route_id = r.id
            LEFT JOIN deliveries d ON d.route_id = r.id
            LEFT JOIN source_albums sa ON sa.id = d.album_id
        """
        params: tuple[Any, ...] = ()
        if target_id is not None:
            query += " WHERE r.target_telegram_id = ? AND r.target_topic_id = ?"
            params = (target_id, target_topic_id)
        query += " GROUP BY r.id ORDER BY r.id"
        with self._lock:
            return list(self.conn.execute(query, params).fetchall())

    def list_issues(
        self, target_id: int | None = None, limit: int = 50, target_topic_id: int = 0
    ) -> list[sqlite3.Row]:
        query = """
            SELECT d.id, d.status, d.attempt_count, d.last_error, d.updated_at,
                    r.target_telegram_id, r.target_topic_id,
                    r.target_title, r.target_topic_title,
                   s.telegram_id AS source_telegram_id, a.grouped_id
            FROM deliveries d
            JOIN routes r ON r.id = d.route_id
            JOIN source_albums a ON a.id = d.album_id
            JOIN sources s ON s.id = a.source_id
            WHERE d.status IN ('failed', 'ambiguous')
        """
        params: list[Any] = []
        if target_id is not None:
            query += " AND r.target_telegram_id = ? AND r.target_topic_id = ?"
            params.extend((target_id, target_topic_id))
        query += " ORDER BY d.updated_at DESC, d.id DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            return list(self.conn.execute(query, tuple(params)).fetchall())

    def delivery_counts(
        self, target_id: int | None = None, target_topic_id: int = 0
    ) -> dict[str, int]:
        query = """
            SELECT d.status, COUNT(*) AS count FROM deliveries d
            JOIN routes r ON r.id = d.route_id
        """
        params: tuple[Any, ...] = ()
        if target_id is not None:
            query += " WHERE r.target_telegram_id = ? AND r.target_topic_id = ?"
            params = (target_id, target_topic_id)
        query += " GROUP BY d.status"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
            return {str(row["status"]): int(row["count"]) for row in rows}

    def set_threshold(
        self, target_id: int, threshold: int, target_topic_id: int = 0
    ) -> bool:
        if threshold <= 0:
            raise ValueError("阈值必须大于 0")
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE alert_settings SET threshold = ?
                WHERE route_id = (
                    SELECT id FROM routes
                    WHERE target_telegram_id = ? AND target_topic_id = ?
                )
                """,
                (threshold, target_id, target_topic_id),
            )
            return cursor.rowcount > 0

    def set_alert_enabled(
        self, target_id: int, enabled: bool, target_topic_id: int = 0
    ) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE alert_settings SET enabled = ?
                WHERE route_id = (
                    SELECT id FROM routes
                    WHERE target_telegram_id = ? AND target_topic_id = ?
                )
                """,
                (int(enabled), target_id, target_topic_id),
            )
            return cursor.rowcount > 0

    def mark_alerted(self, route_ids: list[int], day: date) -> None:
        if not route_ids:
            return
        with self.transaction() as conn:
            conn.executemany(
                "UPDATE alert_settings SET last_alert_date = ? WHERE route_id = ?",
                [(day.isoformat(), route_id) for route_id in route_ids],
            )

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT value FROM service_settings WHERE key = ?", (key,)
            ).fetchone()
            return str(row["value"]) if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO service_settings(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def status_counts(self) -> dict[str, int]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT status, COUNT(*) AS count FROM deliveries GROUP BY status"
            ).fetchall()
            return {str(row["status"]): int(row["count"]) for row in rows}
