from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
from typing import Any, Awaitable, Callable

from telethon import TelegramClient, errors, events, functions, types, utils

from .db import Database
from .timeutils import parse_datetime, utc_now

logger = logging.getLogger(__name__)


class SourceAlbumUnavailable(Exception):
    pass


class TargetTopicUnavailable(Exception):
    pass


class AlbumCollector:
    def __init__(self, client: TelegramClient, db: Database):
        self.client = client
        self.db = db
        self._backfill_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self._history_requested = asyncio.Event()
        self.notify_scan_result: Callable[[int, str], Awaitable[None]] | None = None

    def request_history_scan(self) -> None:
        self._history_requested.set()

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
                while True:
                    try:
                        await self.backfill_source(source)
                        break
                    except errors.FloodWaitError as exc:
                        logger.warning("补录触发限流，等待 %s 秒", exc.seconds)
                        await asyncio.sleep(exc.seconds)
                    except Exception as exc:
                        self.db.set_forward_scan_state(
                            int(source["telegram_id"]), "failed", str(exc)[:1000]
                        )
                        logger.exception("源频道补录失败 source=%s", source["telegram_id"])
                        break

    async def backfill_source(self, source: Any) -> None:
        chat_id = int(source["telegram_id"])
        watermark = int(source["last_message_id"])
        self.db.set_forward_scan_state(chat_id, "scanning")
        current_group: list[Any] = []
        current_group_id: str | None = None
        maximum = watermark
        try:
            async for message in self.client.iter_messages(
                chat_id, min_id=watermark, reverse=True, limit=None
            ):
                maximum = max(maximum, int(message.id))
                grouped_id = str(message.grouped_id) if message.grouped_id is not None else None
                if current_group and grouped_id != current_group_id:
                    self._ingest_group(chat_id, current_group)
                    current_group = []
                    current_group_id = None
                if grouped_id is not None:
                    current_group_id = grouped_id
                    current_group.append(message)
            if current_group:
                self._ingest_group(chat_id, current_group)
            if maximum > watermark:
                self.db.update_watermark(chat_id, maximum)
            self.db.set_forward_scan_state(chat_id, "idle")
            await self._complete_ready_routes(int(source["id"]))
        except Exception:
            raise

    def _ingest_group(self, chat_id: int, messages: list[Any]) -> bool:
        messages.sort(key=lambda message: message.id)
        if len(messages) < 2:
            return False
        self.db.ingest_album(
            chat_id,
            messages[0].grouped_id,
            [message.id for message in messages],
            messages[0].date,
        )
        return True

    async def periodic_backfill(self, interval_seconds: float = 300.0) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            await self.backfill_all()

    async def history_worker(self, interval_seconds: float = 300.0) -> None:
        while True:
            self._history_requested.clear()
            await self.backfill_history_all()
            try:
                await asyncio.wait_for(
                    self._history_requested.wait(), timeout=interval_seconds
                )
            except TimeoutError:
                pass

    async def backfill_history_all(self) -> None:
        async with self._history_lock:
            for source in self.db.list_history_sources():
                while True:
                    try:
                        await self.backfill_history_source(source)
                        break
                    except errors.FloodWaitError as exc:
                        logger.warning("历史扫描触发限流，等待 %s 秒", exc.seconds)
                        await asyncio.sleep(exc.seconds)
                        refreshed = self.db.get_source(int(source["telegram_id"]), enabled_only=True)
                        if refreshed is None:
                            break
                        source = refreshed
                    except Exception as exc:
                        requester, failed_routes = self.db.fail_history_scan(
                            int(source["telegram_id"]), str(exc)
                        )
                        logger.exception("源频道历史扫描失败 source=%s", source["telegram_id"])
                        if requester and self.notify_scan_result:
                            await self.notify_scan_result(
                                requester,
                                f"❌ 历史扫描失败：{source['title']} ({source['telegram_id']})\n{exc}",
                            )
                        if self.notify_scan_result:
                            for route in failed_routes:
                                topic = (
                                    f" / {route['target_topic_title']}"
                                    if route["target_topic_title"]
                                    else ""
                                )
                                await self.notify_scan_result(
                                    int(route["backfill_requested_by"]),
                                    f"❌ 目标回填失败：{route['target_title']}{topic}\n{exc}",
                                )
                        break

    async def backfill_history_source(self, source: Any) -> None:
        chat_id = int(source["telegram_id"])
        active = self.db.begin_history_scan(chat_id)
        if not active or active["history_requested_min_id"] is None:
            return
        requested = int(active["history_requested_min_id"])
        covered = int(active["history_covered_from_id"])
        generation = int(active["scan_generation"])
        requested = await self._resolve_album_boundary(chat_id, requested)
        self.db.lower_history_request(int(active["id"]), requested)

        current_group: list[Any] = []
        current_group_id: str | None = None
        processed = 0
        safe_covered = covered
        async for message in self.client.iter_messages(
            chat_id,
            min_id=requested - 1,
            max_id=covered,
            reverse=False,
            limit=None,
        ):
            grouped_id = str(message.grouped_id) if message.grouped_id is not None else None
            if current_group and grouped_id != current_group_id:
                self._ingest_group(chat_id, current_group)
                safe_covered = min(int(item.id) for item in current_group)
                current_group = []
                current_group_id = None
            if grouped_id is not None:
                current_group_id = grouped_id
                current_group.append(message)
            else:
                safe_covered = int(message.id)
            processed += 1
            if processed % 100 == 0:
                if not self.db.checkpoint_history(chat_id, safe_covered, generation):
                    return
                await self._complete_ready_routes(int(active["id"]))

        if current_group:
            self._ingest_group(chat_id, current_group)
        if not self.db.checkpoint_history(chat_id, requested, generation):
            return
        requester = self.db.finish_history_scan(chat_id, generation)
        await self._complete_ready_routes(int(active["id"]))
        if requester and self.notify_scan_result:
            refreshed = self.db.get_source(chat_id)
            await self.notify_scan_result(
                requester,
                f"✅ 历史扫描完成：{active['title']} ({chat_id})\n"
                f"最早覆盖消息：{refreshed['history_covered_from_id']}",
            )

    async def _resolve_album_boundary(self, chat_id: int, start_id: int) -> int:
        boundary = await self.client.get_messages(chat_id, ids=start_id)
        if boundary is None or getattr(boundary, "grouped_id", None) is None:
            return start_id
        candidates = await self.client.get_messages(
            chat_id, ids=list(range(max(1, start_id - 9), start_id + 1))
        )
        matching = [
            int(message.id)
            for message in candidates
            if message is not None and message.grouped_id == boundary.grouped_id
        ]
        return min(matching) if matching else start_id

    async def _complete_ready_routes(self, source_id: int) -> None:
        for route in self.db.complete_ready_backfills(source_id):
            requester = route["backfill_requested_by"]
            if requester and self.notify_scan_result:
                topic = (
                    f" / {route['target_topic_title']}"
                    if route["target_topic_title"]
                    else ""
                )
                await self.notify_scan_result(
                    int(requester),
                    f"✅ 目标回填完成：{route['target_title']}{topic}\n"
                    f"起点消息：{route['delivery_start_message_id']}",
                )


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
                await self._ensure_topic_available(delivery)
                sent = await self._send_with_flood_wait(delivery, messages)
                if not isinstance(sent, (list, tuple)):
                    sent = [sent]
                self.db.mark_delivery(
                    int(delivery["id"]),
                    "sent",
                    target_message_ids=[int(message.id) for message in sent if message],
                )
                logger.info(
                    "媒体组发布成功 delivery=%s target=%s topic=%s",
                    delivery["id"],
                    delivery["target_telegram_id"],
                    delivery["target_topic_id"],
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
                kwargs: dict[str, Any] = {
                    "file": files,
                    "caption": captions,
                    "formatting_entities": formatting_entities,
                }
                topic_id = int(delivery["target_topic_id"] or 0)
                if topic_id:
                    kwargs["reply_to"] = topic_id
                return await self.client.send_file(
                    int(delivery["target_telegram_id"]),
                    **kwargs,
                )
            except errors.FloodWaitError as exc:
                logger.warning("Telegram 限流，等待 %s 秒", exc.seconds)
                await asyncio.sleep(exc.seconds)

    async def _ensure_topic_available(self, delivery: Any) -> None:
        topic_id = int(delivery["target_topic_id"] or 0)
        if not topic_id:
            return
        while True:
            try:
                result = await self.client(
                    functions.messages.GetForumTopicsByIDRequest(
                        peer=int(delivery["target_telegram_id"]),
                        topics=[topic_id],
                    )
                )
                break
            except errors.FloodWaitError as exc:
                logger.warning("验证目标话题触发限流，等待 %s 秒", exc.seconds)
                await asyncio.sleep(exc.seconds)
        topic = next(
            (
                item
                for item in getattr(result, "topics", [])
                if not isinstance(item, types.ForumTopicDeleted)
                and int(getattr(item, "id", 0)) == topic_id
                and getattr(item, "title", None)
            ),
            None,
        )
        if topic is None:
            raise TargetTopicUnavailable(f"目标话题 {topic_id} 已删除或不存在")
        if getattr(topic, "closed", False):
            raise TargetTopicUnavailable(f"目标话题 {topic_id} 已关闭")
        if getattr(topic, "hidden", False):
            raise TargetTopicUnavailable(f"目标话题 {topic_id} 已隐藏")


def canonical_peer_id(entity: Any) -> int:
    return int(utils.get_peer_id(entity))
