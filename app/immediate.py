from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from .db import Database
from .targets import target_display
from .telegram_service import AlbumPublisher


logger = logging.getLogger(__name__)
NoticeCallback = Callable[[int, str], Awaitable[None]]


class ImmediateSendRunner:
    """Resume and execute persistent immediate-send batches in FIFO job order."""

    def __init__(
        self,
        db: Database,
        publisher: AlbumPublisher,
        notify: NoticeCallback,
        *,
        poll_seconds: float = 5.0,
    ) -> None:
        self.db = db
        self.publisher = publisher
        self.notify = notify
        self.poll_seconds = poll_seconds
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()

    def wake(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            self._wake.clear()
            try:
                did_work = await self.run_once()
                if did_work:
                    continue
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("立即发送任务循环异常")
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass

    async def run_once(self) -> bool:
        notice = self.db.next_unnotified_immediate_job()
        if notice:
            try:
                await self.notify(int(notice["requester_id"]), self.format_notice(notice))
            finally:
                # A failed proactive message must not block publishing or retry forever.
                self.db.mark_immediate_job_notified(int(notice["id"]))
            return True

        job = self.db.next_runnable_immediate_job()
        if not job:
            return False
        if job["status"] == "cancel_requested":
            self.db.cancel_immediate_job(int(job["id"]))
            self.db.finish_immediate_job(int(job["id"]))
        else:
            await self.publisher.publish_immediate_job(int(job["id"]))
        return True

    @staticmethod
    def format_notice(row: Any) -> str:
        label = "已取消" if row["status"] == "cancelled" else "已完成"
        return (
            f"✅ 立即发送任务{label}：#{row['id']}\n"
            f"目标：{target_display(row)}\n"
            f"请求 {row['requested_count']} 组，安排 {row['planned_count']} 组；"
            f"成功={row['sent_count']}，失败={row['failed_count']}，"
            f"源取消={row['cancelled_count']}，状态不明={row['ambiguous_count']}，"
            f"已释放={row['released_count']}。"
        )
