from __future__ import annotations

import asyncio
import logging

from telethon import TelegramClient

from .config import Config
from .db import Database
from .manager_bot import ManagerBot
from .scheduler import ScheduleRunner
from .telegram_service import AlbumCollector, AlbumPublisher

logger = logging.getLogger(__name__)


async def login(config: Config) -> None:
    client = TelegramClient(str(config.user_session), config.api_id, config.api_hash)
    await client.start(phone=config.phone)
    me = await client.get_me()
    print(f"Telegram 用户账号登录成功: {me.id}")
    await client.disconnect()


async def run_service(config: Config) -> None:
    db = Database(config.database_path)
    db.initialize()
    interrupted = db.recover_interrupted()
    if interrupted:
        logger.warning("发现 %d 条发送中断任务，已标记 ambiguous", interrupted)

    user = TelegramClient(str(config.user_session), config.api_id, config.api_hash)
    bot = TelegramClient(str(config.bot_session), config.api_id, config.api_hash)
    tasks: list[asyncio.Task[object]] = []
    try:
        await user.connect()
        if not await user.is_user_authorized():
            raise RuntimeError("用户账号尚未登录，请先运行: python -m app login")

        collector = AlbumCollector(user, db)
        collector.register_handlers()
        # Reconcile source history before schedules are allowed to consume missed slots.
        await collector.backfill_all()

        manager = ManagerBot(
            bot,
            user,
            db,
            config.admin_user_ids,
            history_wakeup=collector.request_history_scan,
        )
        collector.notify_scan_result = manager.send_scan_notice
        manager.register_handlers()
        await bot.start(bot_token=config.bot_token)
        await manager.sync_command_menu()

        publisher = AlbumPublisher(user, db)
        scheduler = ScheduleRunner(db, publisher.publish_for_slot, manager.send_inventory_alerts)
        tasks = [
            asyncio.create_task(scheduler.run(), name="scheduler"),
            asyncio.create_task(collector.periodic_backfill(), name="backfill"),
            asyncio.create_task(collector.history_worker(), name="history-backfill"),
        ]
        logger.info("服务已启动")
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await bot.disconnect()
        await user.disconnect()
        db.close()
