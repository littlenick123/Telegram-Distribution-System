from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from app.db import Database
from app.telegram_service import AlbumCollector, AlbumPublisher


class FakeTelegramClient:
    def __init__(self, messages: list[object]):
        self.messages = messages
        self.send_calls: list[tuple[int, dict[str, object]]] = []

    async def get_messages(self, entity: int, ids: list[int]):
        wanted = set(ids)
        return [message for message in self.messages if message.id in wanted]

    async def send_file(self, entity: int, **kwargs: object):
        self.send_calls.append((entity, kwargs))
        return [SimpleNamespace(id=900 + index) for index, _ in enumerate(kwargs["file"])]

    def iter_messages(self, entity: int, **kwargs: object):
        async def iterator():
            for message in self.messages:
                if message.id > kwargs.get("min_id", 0):
                    yield message
        return iterator()


def message(message_id: int, grouped_id: int | None, caption: str = "") -> object:
    return SimpleNamespace(
        id=message_id,
        grouped_id=grouped_id,
        date=datetime(2026, 7, 15, 1, 0, tzinfo=timezone.utc),
        media=f"media-{message_id}" if grouped_id is not None else None,
        message=caption,
        entities=[f"entity-{message_id}"] if caption else [],
    )


class TelegramServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "test.sqlite3")
        self.db.initialize()
        self.db.add_source(-1001, "源频道", 10)
        self.db.add_route(-1001, -2001, "目标频道")

    async def asyncTearDown(self) -> None:
        self.db.close()
        self.tempdir.cleanup()

    async def test_backfill_groups_albums_and_ignores_single_media(self) -> None:
        client = FakeTelegramClient([
            message(11, 500, "第一项"),
            message(12, 500, "第二项"),
            message(13, None, "单帖"),
        ])
        collector = AlbumCollector(client, self.db)
        await collector.backfill_source(self.db.get_source(-1001))
        self.assertEqual(self.db.inventory(-2001)[0]["pending_count"], 1)
        self.assertEqual(self.db.get_source(-1001)["last_message_id"], 13)

    async def test_publisher_copies_album_without_forwarding(self) -> None:
        messages = [message(11, 500, "第一项"), message(12, 500, "第二项")]
        client = FakeTelegramClient(messages)
        self.db.ingest_album(-1001, 500, [11, 12], messages[0].date)
        route = self.db.get_route_by_target(-2001)
        publisher = AlbumPublisher(client, self.db, min_target_interval=0)
        slot = {"route_id": route["id"], "target_telegram_id": -2001}
        await publisher.publish_for_slot(slot, datetime(2026, 7, 16, tzinfo=timezone.utc))

        self.assertEqual(len(client.send_calls), 1)
        target, kwargs = client.send_calls[0]
        self.assertEqual(target, -2001)
        self.assertEqual(kwargs["file"], ["media-11", "media-12"])
        self.assertEqual(kwargs["caption"], ["第一项", "第二项"])
        self.assertEqual(kwargs["formatting_entities"], [["entity-11"], ["entity-12"]])
        self.assertEqual(self.db.delivery_counts(-2001), {"sent": 1})

    async def test_incomplete_source_album_cancels_all_copies(self) -> None:
        complete = [message(11, 500), message(12, 500)]
        self.db.ingest_album(-1001, 500, [11, 12], complete[0].date)
        client = FakeTelegramClient([complete[0]])
        route = self.db.get_route_by_target(-2001)
        publisher = AlbumPublisher(client, self.db, min_target_interval=0)
        await publisher.publish_for_slot(
            {"route_id": route["id"], "target_telegram_id": -2001},
            datetime(2026, 7, 16, tzinfo=timezone.utc),
        )
        self.assertEqual(client.send_calls, [])
        self.assertEqual(self.db.delivery_counts(-2001), {"cancelled": 1})


if __name__ == "__main__":
    unittest.main()
