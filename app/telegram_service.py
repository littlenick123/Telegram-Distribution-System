from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import json
import logging
from typing import Any

from telethon import TelegramClient, errors, events, utils

from .db import Database
from .timeutils import parse_datetime, utc_now

logger = logging.getLogger(__name__)


class SourceAlbumUnavailable(Exception):
    pass


class AlbumCollector:
    def __init__(self, client: TelegramClient, db: Database):
        self.client = client
        self.db = db
        self._backfill_lock = asyncio.Lock()

    def register_handlers(self) -> None:
        self.client.add_event_handler(self._on_album, events.Album())
        self.client.add_event_handler(self._on_deleted, events.MessageDeleted())

    async def _on_album(self, event: events.Album.Event) -> None:
        chat_id = event.chat_id
        if chat_id is None or not self.db.get_source(chat_id, enabled_only=True):
            return
        messages = sorted(event.messages, key=lambda message: message.id)
        if len(messages) < 2 or messages[0].grouped_id is None:
            return
        try:
            _, created = self.db.ingest_album(
                chat_id,
                messages[0].grouped_id,
                [message.id for message in messages],
                messages[0].date,
            )
            if created:
                logger.info("媒体组已入库 source=%s grouped_id=%s items=%d", chat_id, messages[0].grouped_id, len(messages))
        except Exception:
            logger.exception("实时媒体组入库失败 source=%s", chat_id)

    async def _on_deleted(self, event: events.MessageDeleted.Event) -> None:
        chat_id = event.chat_id
        if chat_id is None or not self.db.get_source(chat_id):
            return
        cancelled = self.db.cancel_albums_containing(chat_id, list(event.deleted_ids))
        if cancelled:
            logger.warning("源消息删除，已取消 %d 个媒体组 source=%s", cancelled, chat_id)

    async def backfill_all(self) -> None:
        async with self._backfill_lock:
            for source in self.db.list_sources(enabled_only=True):
                try:
                    await self.backfill_source(source)
                except errors.FloodWaitError as exc:
                    logger.warning("补录触发限流，等待 %s 秒", exc.seconds)
                    await asyncio.sleep(exc.seconds)
                    await self.backfill_source(source)
                except Exception:
                    logger.exception("源频道补录失败 source=%s", source["telegram_id"])

    async def backfill_source(self, source: Any) -> None:
        chat_id = int(source["telegram_id"])
        watermark = int(source["last_message_id"])
        groups: dict[str, list[Any]] = defaultdict(list)
        maximum = watermark
        async for message in self.client.iter_messages(
            chat_id, min_id=watermark, reverse=True, limit=None
        ):
            maximum = max(maximum, message.id)
            if message.grouped_id is not None:
                groups[str(message.grouped_id)].append(message)

        ordered_groups = sorted(
            groups.values(), key=lambda items: min(message.id for message in items)
        )
        for messages in ordered_groups:
            messages.sort(key=lambda message: message.id)
            if len(messages) < 2:
                # Never advance past a partial group. It will be retried next pass.
                maximum = min(maximum, messages[0].id - 1)
                continue
            self.db.ingest_album(
                chat_id,
                messages[0].grouped_id,
                [message.id for message in messages],
                messages[0].date,
            )
        if maximum > watermark:
            self.db.update_watermark(chat_id, maximum)

    async def periodic_backfill(self, interval_seconds: float = 300.0) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            await self.backfill_all()


class AlbumPublisher:
    def __init__(self, client: TelegramClient, db: Database, min_target_interval: int = 60):
        self.client = client
        self.db = db
        self.min_target_interval = min_target_interval
        self._send_lock = asyncio.Lock()

    async def publish_for_slot(self, slot: Any, occurrence_utc: datetime) -> None:
        delivery = self.db.claim_next_delivery(int(slot["route_id"]), occurrence_utc)
        if not delivery:
            logger.info("发布时间队列为空 target=%s time=%s", slot["target_telegram_id"], occurrence_utc.isoformat())
            return
        await self.publish(delivery)

    async def publish(self, delivery: Any) -> None:
        async with self._send_lock:
            await self._respect_target_interval(delivery["last_sent_at"])
            try:
                messages = await self._load_complete_album(delivery)
                sent = await self._send_with_flood_wait(delivery, messages)
                if not isinstance(sent, (list, tuple)):
                    sent = [sent]
                self.db.mark_delivery(
                    int(delivery["id"]),
                    "sent",
                    target_message_ids=[int(message.id) for message in sent if message],
                )
                logger.info(
                    "媒体组发布成功 delivery=%s target=%s",
                    delivery["id"],
                    delivery["target_telegram_id"],
                )
            except SourceAlbumUnavailable as exc:
                self.db.cancel_album(int(delivery["album_id"]), str(exc))
                logger.warning("源媒体组不可用 delivery=%s: %s", delivery["id"], exc)
            except asyncio.CancelledError:
                # Leave it in sending; next startup will conservatively mark it ambiguous.
                raise
            except Exception as exc:
                self.db.mark_delivery(int(delivery["id"]), "failed", error=str(exc)[:1000])
                logger.exception("媒体组发布失败 delivery=%s", delivery["id"])

    async def _respect_target_interval(self, last_sent_at: str | None) -> None:
        if not last_sent_at:
            return
        elapsed = (utc_now() - parse_datetime(last_sent_at).astimezone(timezone.utc)).total_seconds()
        remaining = self.min_target_interval - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)

    async def _load_complete_album(self, delivery: Any) -> list[Any]:
        ids = [int(value) for value in json.loads(delivery["message_ids_json"])]
        fetched = await self.client.get_messages(
            int(delivery["source_telegram_id"]), ids=ids
        )
        by_id = {message.id: message for message in fetched if message is not None}
        if len(by_id) != len(ids):
            raise SourceAlbumUnavailable("源媒体组已删除或成员不完整")
        messages = [by_id[message_id] for message_id in ids]
        grouped_ids = {str(message.grouped_id) for message in messages}
        if grouped_ids != {str(delivery["grouped_id"])} or any(message.media is None for message in messages):
            raise SourceAlbumUnavailable("源媒体组结构已变化")
        return messages

    async def _send_with_flood_wait(self, delivery: Any, messages: list[Any]) -> Any:
        files = [message.media for message in messages]
        captions = [message.message or "" for message in messages]
        formatting_entities = [message.entities or [] for message in messages]
        while True:
            try:
                return await self.client.send_file(
                    int(delivery["target_telegram_id"]),
                    file=files,
                    caption=captions,
                    formatting_entities=formatting_entities,
                )
            except errors.FloodWaitError as exc:
                logger.warning("Telegram 限流，等待 %s 秒", exc.seconds)
                await asyncio.sleep(exc.seconds)


def canonical_peer_id(entity: Any) -> int:
    return int(utils.get_peer_id(entity))
