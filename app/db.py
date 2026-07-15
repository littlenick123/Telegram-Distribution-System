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
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    target_telegram_id INTEGER NOT NULL UNIQUE,
    target_title TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    last_sent_at TEXT,
    created_at TEXT NOT NULL
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
            count = self.conn.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0]
            if not count:
                self.conn.execute("INSERT INTO schema_version(version) VALUES (1)")
            self.conn.execute(
                "INSERT OR IGNORE INTO service_settings(key, value) VALUES ('timezone', 'Asia/Shanghai')"
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO service_settings(key, value) VALUES ('alert_time', '09:00')"
            )
            self.conn.commit()

    def add_source(self, telegram_id: int, title: str, watermark: int) -> int:
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT id FROM sources WHERE telegram_id = ?", (telegram_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE sources SET title = ?, enabled = 1 WHERE id = ?",
                    (title, existing["id"]),
                )
                return int(existing["id"])
            cursor = conn.execute(
                "INSERT INTO sources(telegram_id, title, last_message_id, created_at) VALUES (?, ?, ?, ?)",
                (telegram_id, title, watermark, now),
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

    def add_route(self, source_telegram_id: int, target_id: int, target_title: str) -> int:
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            source = conn.execute(
                "SELECT id FROM sources WHERE telegram_id = ?", (source_telegram_id,)
            ).fetchone()
            if not source:
                raise ValueError("源频道尚未添加")
            existing = conn.execute(
                "SELECT id, source_id FROM routes WHERE target_telegram_id = ?", (target_id,)
            ).fetchone()
            if existing:
                if existing["source_id"] != source["id"]:
                    raise ValueError("该目标频道已经映射到另一个源频道")
                conn.execute(
                    "UPDATE routes SET target_title = ?, enabled = 1 WHERE id = ?",
                    (target_title, existing["id"]),
                )
                return int(existing["id"])
            cursor = conn.execute(
                "INSERT INTO routes(source_id, target_telegram_id, target_title, created_at) VALUES (?, ?, ?, ?)",
                (source["id"], target_id, target_title, now),
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

    def get_route_by_target(self, target_id: int) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                """
                SELECT r.*, s.telegram_id AS source_telegram_id, s.title AS source_title
                FROM routes r JOIN sources s ON s.id = r.source_id
                WHERE r.target_telegram_id = ?
                """,
                (target_id,),
            ).fetchone()

    def set_route_enabled(self, target_id: int, enabled: bool) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE routes SET enabled = ? WHERE target_telegram_id = ?",
                (int(enabled), target_id),
            )
            return cursor.rowcount > 0

    def add_schedule(self, target_id: int, hhmm: str, initial_date: date) -> int:
        return self.add_schedules(target_id, [(hhmm, initial_date)])[0][0]

    def add_schedules(
        self, target_id: int, schedules: list[tuple[str, date]]
    ) -> list[tuple[int, str, bool]]:
        """Atomically add schedules, returning (slot_id, hhmm, created)."""
        if not schedules:
            raise ValueError("至少需要一个发布时间")
        with self.transaction() as conn:
            route = conn.execute(
                "SELECT id FROM routes WHERE target_telegram_id = ?", (target_id,)
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
        self, target_id: int, times: list[str] | None
    ) -> tuple[list[str], list[str]]:
        """Delete all target schedules when times is None, otherwise selected times."""
        with self.transaction() as conn:
            route = conn.execute(
                "SELECT id FROM routes WHERE target_telegram_id = ?", (target_id,)
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
        self, enabled_only: bool = False, target_id: int | None = None
    ) -> list[sqlite3.Row]:
        query = """
            SELECT sl.id, sl.route_id, sl.time_hhmm, sl.enabled, st.last_processed_date,
                   r.target_telegram_id, r.target_title
            FROM schedule_slots sl
            JOIN schedule_state st ON st.slot_id = sl.id
            JOIN routes r ON r.id = sl.route_id
            JOIN sources s ON s.id = r.source_id
        """
        conditions: list[str] = []
        params: list[Any] = []
        if enabled_only:
            conditions.append("sl.enabled = 1 AND r.enabled = 1 AND s.enabled = 1")
        if target_id is not None:
            conditions.append("r.target_telegram_id = ?")
            params.append(target_id)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY r.target_telegram_id, sl.time_hhmm, sl.id"
        with self._lock:
            return list(self.conn.execute(query, tuple(params)).fetchall())

    def backfill_route(self, target_id: int) -> tuple[int, int, int]:
        """Create missing pending deliveries for every active album in a route's source."""
        now = to_utc_iso(utc_now())
        with self.transaction() as conn:
            route = conn.execute(
                "SELECT id, source_id FROM routes WHERE target_telegram_id = ?", (target_id,)
            ).fetchone()
            if not route:
                raise ValueError("目标频道尚未映射")
            album_rows = conn.execute(
                """
                SELECT id FROM source_albums
                WHERE source_id = ? AND status = 'active'
                ORDER BY source_date, id
                """,
                (route["source_id"],),
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
            conn.execute(
                "UPDATE sources SET last_message_id = MAX(last_message_id, ?) WHERE id = ?",
                (max(ids), source["id"]),
            )
            existing = conn.execute(
                "SELECT id FROM source_albums WHERE source_id = ? AND grouped_id = ?",
                (source["id"], str(grouped_id)),
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE source_albums SET message_ids_json = ?, source_date = ?, updated_at = ? WHERE id = ?",
                    (json.dumps(ids), to_utc_iso(source_date), now, existing["id"]),
                )
                return int(existing["id"]), False
            cursor = conn.execute(
                """
                INSERT INTO source_albums(
                    source_id, grouped_id, message_ids_json, source_date, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source["id"], str(grouped_id), json.dumps(ids), to_utc_iso(source_date), now, now),
            )
            album_id = int(cursor.lastrowid)
            routes = conn.execute(
                "SELECT id FROM routes WHERE source_id = ? AND enabled = 1", (source["id"],)
            ).fetchall()
            conn.executemany(
                "INSERT OR IGNORE INTO deliveries(album_id, route_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
                [(album_id, route["id"], now, now) for route in routes],
            )
            return album_id, True

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
                WHERE d.route_id = ? AND d.status = 'pending' AND a.status = 'active'
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
                       r.target_telegram_id, r.target_title, r.last_sent_at
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

    def inventory(self, target_id: int | None = None) -> list[sqlite3.Row]:
        query = """
            SELECT r.id AS route_id, r.target_telegram_id, r.target_title,
                   r.enabled AS route_enabled, s.enabled AS source_enabled,
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
            query += " WHERE r.target_telegram_id = ?"
            params = (target_id,)
        query += " GROUP BY r.id ORDER BY r.id"
        with self._lock:
            return list(self.conn.execute(query, params).fetchall())

    def list_issues(self, target_id: int | None = None, limit: int = 50) -> list[sqlite3.Row]:
        query = """
            SELECT d.id, d.status, d.attempt_count, d.last_error, d.updated_at,
                   r.target_telegram_id, r.target_title,
                   s.telegram_id AS source_telegram_id, a.grouped_id
            FROM deliveries d
            JOIN routes r ON r.id = d.route_id
            JOIN source_albums a ON a.id = d.album_id
            JOIN sources s ON s.id = a.source_id
            WHERE d.status IN ('failed', 'ambiguous')
        """
        params: list[Any] = []
        if target_id is not None:
            query += " AND r.target_telegram_id = ?"
            params.append(target_id)
        query += " ORDER BY d.updated_at DESC, d.id DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            return list(self.conn.execute(query, tuple(params)).fetchall())

    def delivery_counts(self, target_id: int | None = None) -> dict[str, int]:
        query = """
            SELECT d.status, COUNT(*) AS count FROM deliveries d
            JOIN routes r ON r.id = d.route_id
        """
        params: tuple[Any, ...] = ()
        if target_id is not None:
            query += " WHERE r.target_telegram_id = ?"
            params = (target_id,)
        query += " GROUP BY d.status"
        with self._lock:
            rows = self.conn.execute(query, params).fetchall()
            return {str(row["status"]): int(row["count"]) for row in rows}

    def set_threshold(self, target_id: int, threshold: int) -> bool:
        if threshold <= 0:
            raise ValueError("阈值必须大于 0")
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE alert_settings SET threshold = ?
                WHERE route_id = (SELECT id FROM routes WHERE target_telegram_id = ?)
                """,
                (threshold, target_id),
            )
            return cursor.rowcount > 0

    def set_alert_enabled(self, target_id: int, enabled: bool) -> bool:
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                UPDATE alert_settings SET enabled = ?
                WHERE route_id = (SELECT id FROM routes WHERE target_telegram_id = ?)
                """,
                (int(enabled), target_id),
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
