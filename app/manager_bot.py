from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
import logging
import re
import shlex
from typing import Any, Callable, Iterable

from telethon import TelegramClient, events, functions, types

from .db import Database
from .targets import TargetRef, target_display
from .telegram_service import canonical_peer_id
from .timeutils import initial_schedule_date, utc_now, validate_hhmm

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BotCommandSpec:
    name: str
    description: str
    min_args: int
    syntax: str
    example: str

    def usage_message(self) -> str:
        return (
            f"命令需要参数：/{self.name}\n\n"
            f"用途：{self.description}\n"
            f"格式：{self.syntax}\n"
            f"示例：{self.example}"
        )


BOT_COMMAND_SPECS = (
    BotCommandSpec("start", "开始使用并查看完整引导", 0, "/start", "/start"),
    BotCommandSpec("help", "查看完整分类命令帮助", 0, "/help", "/help"),
    BotCommandSpec("source_add", "添加源频道并可扫描历史", 1, "/source_add <源频道ID> [all|消息ID]", "/source_add -1001234567890 all"),
    BotCommandSpec("source_list", "查看所有源频道", 0, "/source_list", "/source_list"),
    BotCommandSpec("source_enable", "启用源频道", 1, "/source_enable <源频道ID>", "/source_enable -1001234567890"),
    BotCommandSpec("source_disable", "停用源频道", 1, "/source_disable <源频道ID>", "/source_disable -1001234567890"),
    BotCommandSpec("route_add", "添加目标并可立即回填历史", 2, "/route_add <源频道ID> <目标标识> [all|消息ID]", "/route_add -1001234567890 -1009876543210 all"),
    BotCommandSpec("route_list", "查看全部频道映射", 0, "/route_list", "/route_list"),
    BotCommandSpec("route_enable", "启用目标映射", 1, "/route_enable <目标标识>", "/route_enable -1009876543210:135"),
    BotCommandSpec("route_disable", "停用目标映射", 1, "/route_disable <目标标识>", "/route_disable -1009876543210:135"),
    BotCommandSpec("route_del", "永久删除目标映射及其队列", 1, "/route_del <目标标识>", "/route_del -1009876543210:135"),
    BotCommandSpec("route_backfill", "按独立起点补充目标库存", 2, "/route_backfill <目标标识> <all|消息ID>", "/route_backfill -1009876543210:135 4348"),
    BotCommandSpec("schedule_add", "批量添加每日发布时间", 2, "/schedule_add <目标标识> <HH:MM> [HH:MM ...]", "/schedule_add -1009876543210:135 09:00 13:00 20:00"),
    BotCommandSpec("schedule_list", "查看全部或目标发布时间", 0, "/schedule_list [目标标识]", "/schedule_list -1009876543210:135"),
    BotCommandSpec("schedule_del", "删除一个或多个发布时间", 1, "/schedule_del <计划ID> 或 <目标标识> <HH:MM|all>", "/schedule_del -1009876543210:135 all"),
    BotCommandSpec("threshold", "设置目标低库存阈值", 2, "/threshold <目标标识> <数量>", "/threshold -1009876543210:135 24"),
    BotCommandSpec("alert_enable", "开启目标低库存提醒", 1, "/alert_enable <目标标识>", "/alert_enable -1009876543210:135"),
    BotCommandSpec("alert_disable", "关闭目标低库存提醒", 1, "/alert_disable <目标标识>", "/alert_disable -1009876543210:135"),
    BotCommandSpec("alert_time", "设置每日库存检查时间", 1, "/alert_time <HH:MM>", "/alert_time 09:00"),
    BotCommandSpec("alert_status", "查看全部目标告警状态", 0, "/alert_status", "/alert_status"),
    BotCommandSpec("queue", "查看全部或目标待发库存", 0, "/queue [目标标识]", "/queue -1009876543210:135"),
    BotCommandSpec("status", "查看服务和投递状态", 0, "/status", "/status"),
    BotCommandSpec("issues", "查看失败或状态不明投递", 0, "/issues [目标标识]", "/issues -1009876543210:135"),
    BotCommandSpec("retry", "重新入队失败或不明投递", 1, "/retry <投递ID>", "/retry 42"),
    BotCommandSpec("skip", "取消指定投递", 1, "/skip <投递ID>", "/skip 42"),
)

BOT_COMMANDS_BY_NAME = {f"/{spec.name}": spec for spec in BOT_COMMAND_SPECS}


WELCOME_TEXT = """🎬 欢迎使用影视库管理机器人

本机器人用于管理私密影视库的采集路由、定时发布、库存提醒和异常任务。

首次配置顺序：
1. /source_add -100源频道ID [all|消息ID]
2. /route_add -100源频道ID -100目标频道ID [all|消息ID]
3. /schedule_add -100目标频道ID[:话题ID] 09:00 13:00 20:00
4. /threshold -100目标频道ID[:话题ID] 24
5. /status 检查服务状态

下面将继续发送完整的分类命令说明。"""


HELP_SECTIONS = (
    """📖 使用规则

• 只能在机器人私聊中使用，且发送者必须在 ADMIN_USER_IDS 白名单中。
• <参数> 为必填，[参数] 为可选；输入时不要带尖括号或方括号。
• 频道 ID 通常以 -100 开头，例如 -1001234567890。
• 普通目标标识就是频道 ID；话题目标使用“群组ID:话题ID”，例如 -1009876543210:135。
• 时间使用 24 小时制 HH:MM，统一按 Asia/Shanghai 解释。
• 系统只采集至少包含两项媒体的媒体组；单媒体和纯文本会忽略。
• 历史消息 ID 是源频道消息 ID；话题目标仍使用“群组ID:话题ID”。
• 每个目标拥有独立历史起点，历史扫描不会自动回填所有目标。
• /start 显示首次配置引导和完整帮助；/help 随时重新显示完整帮助。""",
    """📥 源频道与路由

/source_add <源频道ID> [all|消息ID]
不带范围时从当前最新消息后监控；all 扫描全部历史；数字表示包含该消息起扫描。
示例：/source_add -1001234567890
示例：/source_add -1001234567890 all
示例：/source_add -1001234567890 4348

/source_list
列出源频道、启停状态和采集水位。
示例：/source_list

/source_enable <源频道ID>
重新启用源频道；停用期间的新组可能从原水位补录。
示例：/source_enable -1001234567890

/source_disable <源频道ID>
暂停源频道采集及其目标发布，不删除数据库内容。
示例：/source_disable -1001234567890

/route_add <源频道ID> <目标标识> [all|消息ID]
建立普通频道或话题目标映射；不带范围时只接收后续新组，带范围时创建后立即回填。
普通示例：/route_add -1001234567890 -1009876543210
全部历史：/route_add -1001234567890 -1009876543210 all
指定起点：/route_add -1001234567890 -1009876543210 4348
话题示例：/route_add -1001234567890 -1009876543210:135 all
兼容旧格式：/route_add -1001234567890 -1009876543210 135 [all|消息ID]
⚠️ 对话题目标追加历史范围时，推荐使用“群组ID:话题ID”，避免数字含义混淆。

/route_list
列出全部源到目标映射。
示例：/route_list

/route_enable <目标标识>
启用目标映射并继续已有 pending 队列。
示例：/route_enable -1009876543210:135

/route_disable <目标标识>
暂停目标发布及新投递创建，不删除已有记录。
示例：/route_disable -1009876543210:135

/route_del <目标标识>
永久删除目标映射、全部投递历史、发布时间和告警设置；源媒体组及 Telegram 已发帖子不受影响。
示例：/route_del -1009876543210
示例：/route_del -1009876543210:135
⚠️ 本命令不要求确认且无法恢复；如有任务正在发送，会先停用目标并拒绝删除。

/route_backfill <目标标识> <all|消息ID>
为普通频道或话题目标设置独立历史起点并补充库存。
示例：/route_backfill -1009876543210 all
示例：/route_backfill -1009876543210:135 4348
⚠️ 起点只能向更早移动；已有投递状态不会重置。""",
    """⏰ 发布时间管理

/schedule_add <目标标识> <HH:MM> [HH:MM ...]
一次添加一个或多个每日发布时间。
示例：/schedule_add -1009876543210:135 09:00 13:00 20:00

/schedule_list [目标标识]
不带参数查看全部计划，带目标 ID 只查看该目标。
示例：/schedule_list -1009876543210:135

/schedule_del <计划ID>
按 schedule_list 显示的正数计划 ID 删除一条计划。
示例：/schedule_del 3

/schedule_del <目标标识> <HH:MM> [HH:MM ...]
批量删除该目标的指定发布时间。
示例：/schedule_del -1009876543210:135 09:00 13:00

/schedule_del <目标标识> all
删除该目标的全部发布时间。
示例：/schedule_del -1009876543210:135 all
⚠️ all 会立即删除该目标全部计划，但不会删除待发库存。""",
    """📦 库存与提醒

/threshold <目标标识> <数量>
设置目标低库存阈值，必须大于 0；默认 24。
示例：/threshold -1009876543210:135 24

/alert_enable <目标标识>
开启目标的每日低库存提醒。
示例：/alert_enable -1009876543210:135

/alert_disable <目标标识>
关闭目标提醒，不影响正常采集和发布。
示例：/alert_disable -1009876543210:135

/alert_time <HH:MM>
设置全局每日库存检查时间。
示例：/alert_time 09:00

/alert_status
查看所有目标的 pending 数量、阈值和提醒开关。
示例：/alert_status

/queue [目标标识]
查看全部或指定目标的可发布 pending 库存。
示例：/queue -1009876543210:135""",
    """🛠 状态与故障处理

/status
查看启用源、目标映射、各投递状态数量和提醒时间。
示例：/status

/issues [目标标识]
列出最近的 failed 或 ambiguous 投递及投递 ID。
示例：/issues -1009876543210:135

/retry <投递ID>
把 failed 或 ambiguous 投递重新放回 pending 队列。
示例：/retry 42
⚠️ ambiguous 可能已经发出，重试前请先检查目标频道，避免重复。

/skip <投递ID>
取消 pending、failed 或 ambiguous 投递。
示例：/skip 42

/start
显示首次配置引导及全部分类帮助。

/help
重新显示全部分类帮助。""",
)

HELP_TEXT = "\n\n".join(HELP_SECTIONS)
BotResponse = str | list[str]


class ManagerBot:
    def __init__(
        self,
        bot_client: TelegramClient,
        user_client: TelegramClient,
        db: Database,
        admin_ids: Iterable[int],
        history_wakeup: Callable[[], None] | None = None,
    ):
        self.client = bot_client
        self.user_client = user_client
        self.db = db
        self.admin_ids = frozenset(int(value) for value in admin_ids)
        self.history_wakeup = history_wakeup

    def register_handlers(self) -> None:
        self.client.add_event_handler(
            self._handle_command, events.NewMessage(incoming=True, pattern=r"^/")
        )

    async def sync_command_menu(self) -> bool:
        """Synchronize private-chat slash commands without blocking service startup on failure."""
        try:
            self._validate_command_specs()
            result = await self.client(
                functions.bots.SetBotCommandsRequest(
                    scope=types.BotCommandScopeUsers(),
                    lang_code="",
                    commands=[
                        types.BotCommand(command=spec.name, description=spec.description)
                        for spec in BOT_COMMAND_SPECS
                    ],
                )
            )
            if result:
                logger.info("机器人私聊命令菜单已同步，共 %d 条", len(BOT_COMMAND_SPECS))
                return True
            logger.error("机器人私聊命令菜单同步返回失败")
        except Exception:
            logger.exception("机器人私聊命令菜单同步失败，服务将继续运行")
        return False

    async def _handle_command(self, event: events.NewMessage.Event) -> None:
        if not event.is_private:
            return
        if event.sender_id not in self.admin_ids:
            await event.reply("未授权。")
            return
        try:
            parts = shlex.split(event.raw_text)
            command = parts[0].split("@", 1)[0].lower()
            args = parts[1:]
            spec = BOT_COMMANDS_BY_NAME.get(command)
            if spec and len(args) < spec.min_args:
                response = spec.usage_message()
            else:
                response = await self.dispatch(command, args, requester_id=event.sender_id)
        except Exception as exc:
            logger.exception("管理命令执行失败 command=%s", event.raw_text)
            response = f"操作失败：{exc}"
        await self._reply_chunks(event, response)

    async def dispatch(
        self, command: str, args: list[str], requester_id: int | None = None
    ) -> BotResponse:
        if command == "/start":
            return [WELCOME_TEXT, *HELP_SECTIONS]
        if command == "/help":
            return list(HELP_SECTIONS)
        if command == "/source_add":
            self._require_range(args, 1, 2)
            entity = await self.user_client.get_entity(self._peer(args[0]))
            peer_id = canonical_peer_id(entity)
            latest = await self.user_client.get_messages(entity, limit=1)
            watermark = int(latest[0].id) if latest else 0
            history_start: int | None = None
            mode = args[1].lower() if len(args) == 2 else None
            if mode == "all":
                history_start = 1
            elif mode is not None:
                try:
                    history_start = int(mode)
                except ValueError as exc:
                    raise ValueError("历史范围必须是 all 或正整数消息 ID") from exc
                if history_start <= 0:
                    raise ValueError("历史起点消息 ID 必须大于 0")
                if history_start > watermark:
                    raise ValueError(f"历史起点不能超过频道当前最新消息 ID {watermark}")
            self.db.add_source(
                peer_id,
                self._title(entity),
                watermark,
                history_start_id=history_start,
                requested_by=requester_id,
            )
            if history_start is None:
                return f"已添加源频道：{self._title(entity)} ({peer_id})，水位 {watermark}，旧帖不会入库。"
            if self.history_wakeup:
                self.history_wakeup()
            label = "全部历史" if mode == "all" else f"消息 {history_start} 起"
            return (
                f"已添加源频道：{self._title(entity)} ({peer_id})，正在后台扫描{label}。\n"
                "历史媒体组只会进入明确执行过 route_backfill 的目标。"
            )
        if command == "/source_list":
            rows = self.db.list_sources()
            return "源频道：\n" + "\n".join(
                f"{'✅' if row['enabled'] else '⏸'} {row['title']} ({row['telegram_id']}) 水位={row['last_message_id']}"
                f" 正向={row['forward_scan_status']} 历史覆盖={row['history_covered_from_id']}"
                f" 历史={row['history_scan_status']}"
                f"{(' 正向错误=' + row['forward_scan_error']) if row['forward_scan_error'] else ''}"
                f"{(' 错误=' + row['history_scan_error']) if row['history_scan_error'] else ''}"
                for row in rows
            ) if rows else "尚无源频道。"
        if command in {"/source_enable", "/source_disable"}:
            self._require(args, 1)
            enabled = command.endswith("enable")
            found = self.db.set_source_enabled(int(args[0]), enabled)
            return "源频道状态已更新。" if found else "未找到源频道。"
        if command == "/route_add":
            self._require_range(args, 2, 4)
            source_id = int(args[0])
            if not self.db.get_source(source_id):
                raise ValueError("请先添加源频道")
            source_latest = await self.user_client.get_messages(source_id, limit=1)
            latest_id = int(source_latest[0].id) if source_latest else 0
            delivery_start = latest_id + 1

            supplied_target = TargetRef.parse(args[1])
            target = await self.user_client.get_entity(supplied_target.telegram_id)
            await self._verify_can_post(target)
            target_id = canonical_peer_id(target)
            topic_id = supplied_target.topic_id
            topic_title = None

            history_mode: str | None = None
            trailing = args[2:]
            if supplied_target.topic_id:
                if len(trailing) > 1:
                    raise ValueError("组合话题目标后只能再提供一个 all 或消息 ID")
                history_mode = trailing[0] if trailing else None
            elif len(trailing) == 2:
                try:
                    topic_id = int(trailing[0])
                except ValueError as exc:
                    raise ValueError("话题 ID 必须是大于 0 的整数") from exc
                history_mode = trailing[1]
            elif len(trailing) == 1:
                # Keep the old three-argument forum-topic form compatible. For
                # normal channels, the same position is the new history range.
                if getattr(target, "megagroup", False) and trailing[0].lower() != "all":
                    try:
                        topic_id = int(trailing[0])
                    except ValueError as exc:
                        raise ValueError("话题 ID 必须是大于 0 的整数") from exc
                else:
                    history_mode = trailing[0]

            if topic_id:
                topic_title = await self._verify_forum_topic(target, topic_id)

            history_start = self._history_start(history_mode, latest_id, "回填")
            self.db.add_route(
                source_id,
                target_id,
                self._title(target),
                target_topic_id=topic_id,
                target_topic_title=topic_title,
                delivery_start_message_id=delivery_start,
            )
            ref = TargetRef(target_id, topic_id)
            target_name = self._title(target)
            if topic_title:
                target_name += f" / {topic_title}"
            if history_start is None:
                return f"已建立映射：{source_id} → {target_name} ({ref})。仅后续新媒体组会进入该队列。"

            created, existing, pending = self.db.backfill_route(
                target_id,
                topic_id,
                start_message_id=history_start,
                target_message_id=latest_id,
                requested_by=requester_id,
            )
            if self.history_wakeup:
                self.history_wakeup()
            refreshed = self.db.get_route_by_target(target_id, topic_id)
            range_label = "全部历史" if history_mode and history_mode.lower() == "all" else f"消息 {history_start} 起"
            return (
                f"已建立映射：{source_id} → {target_name} ({ref})，并启动{range_label}的库存回填。\n"
                f"已入队新增 {created} 组，已存在 {existing} 组，当前 pending={pending}，"
                f"回填状态={refreshed['backfill_status']}。"
            )
        if command == "/route_list":
            rows = self.db.list_routes()
            return "频道映射：\n" + "\n".join(
                f"{'✅' if row['enabled'] else '⏸'} {row['source_title']} ({row['source_telegram_id']}) → "
                f"{target_display(row)} | 起点={row['delivery_start_message_id']} "
                f"回填={row['backfill_status']}"
                f"{(' 错误=' + row['backfill_error']) if row['backfill_error'] else ''}"
                for row in rows
            ) if rows else "尚无频道映射。"
        if command in {"/route_enable", "/route_disable"}:
            self._require(args, 1)
            enabled = command.endswith("enable")
            target = TargetRef.parse(args[0])
            found = self.db.set_route_enabled(
                target.telegram_id, enabled, target.topic_id
            )
            return "映射状态已更新。" if found else "未找到目标频道。"
        if command == "/route_del":
            self._require(args, 1)
            target = TargetRef.parse(args[0])
            result = self.db.delete_route(target.telegram_id, target.topic_id)
            if result is None:
                return "未找到目标频道，未删除任何数据。"
            counts = result["delivery_counts"]
            count_text = "，".join(
                f"{status}={counts[status]}"
                for status in (
                    "pending", "sending", "sent", "failed", "cancelled", "ambiguous"
                )
                if counts.get(status, 0)
            ) or "无投递记录"
            display = target_display(result)
            if not result["deleted"]:
                return (
                    f"目标已自动停用，但暂未删除：{display}。\n"
                    f"存在 {counts.get('sending', 0)} 条正在发送的投递，请稍后重新执行 "
                    f"/route_del {target}。"
                )
            return (
                f"已永久删除目标映射：{display}。\n"
                f"已清理投递：{count_text}；发布时间={result['schedule_count']}。\n"
                "源媒体组和 Telegram 中已经发布的帖子未删除。"
            )
        if command == "/route_backfill":
            self._require(args, 2)
            target = TargetRef.parse(args[0])
            route = self.db.get_route_by_target(target.telegram_id, target.topic_id)
            if not route:
                raise ValueError("目标频道尚未映射")
            latest = await self.user_client.get_messages(
                int(route["source_telegram_id"]), limit=1
            )
            latest_id = int(latest[0].id) if latest else 0
            start_id = self._history_start(args[1], latest_id, "回填")
            assert start_id is not None
            created, existing, pending = self.db.backfill_route(
                target.telegram_id,
                target.topic_id,
                start_message_id=start_id,
                target_message_id=latest_id,
                requested_by=requester_id,
            )
            if self.history_wakeup:
                self.history_wakeup()
            refreshed = self.db.get_route_by_target(target.telegram_id, target.topic_id)
            return (
                f"库存回填完成：新增 {created} 组，已存在 {existing} 组，"
                f"目标当前 pending={pending}。\n"
                f"独立起点={start_id}，回填状态={refreshed['backfill_status']}。"
            )
        if command == "/schedule_add":
            self._require_at_least(args, 2)
            target = TargetRef.parse(args[0])
            times = self._validated_times(args[1:])
            now = utc_now()
            results = self.db.add_schedules(
                target.telegram_id,
                [(hhmm, initial_schedule_date(hhmm, now)) for hhmm in times],
                target_topic_id=target.topic_id,
            )
            created = [(slot_id, hhmm) for slot_id, hhmm, is_new in results if is_new]
            existing = [(slot_id, hhmm) for slot_id, hhmm, is_new in results if not is_new]
            lines = [f"发布时间处理完成：新增 {len(created)} 个，已存在 {len(existing)} 个。"]
            if created:
                lines.append("新增：" + "，".join(f"#{slot_id} {hhmm}" for slot_id, hhmm in created))
            if existing:
                lines.append("已存在：" + "，".join(f"#{slot_id} {hhmm}" for slot_id, hhmm in existing))
            return "\n".join(lines)
        if command == "/schedule_del":
            self._require_at_least(args, 1)
            if args[0].isdigit():
                identifier = int(args[0])
                self._require(args, 1)
                return "发布时间已删除。" if self.db.delete_schedule(identifier) else "未找到计划。"
            target = TargetRef.parse(args[0])
            if len(args) == 1:
                raise ValueError(
                    "按目标删除时必须提供 HH:MM 或 all，例如: "
                    "/schedule_del -1001234567890 all"
                )
            if args[1].lower() == "all":
                self._require(args, 2)
                deleted, missing = self.db.delete_schedules_by_target(
                    target.telegram_id, None, target.topic_id
                )
            else:
                times = self._validated_times(args[1:])
                deleted, missing = self.db.delete_schedules_by_target(
                    target.telegram_id, times, target.topic_id
                )
            lines = [f"发布时间删除完成：删除 {len(deleted)} 个，未找到 {len(missing)} 个。"]
            if deleted:
                lines.append("已删除：" + "，".join(deleted))
            if missing:
                lines.append("未找到：" + "，".join(missing))
            return "\n".join(lines)
        if command == "/schedule_list":
            if len(args) > 1:
                raise ValueError("格式: /schedule_list [目标标识]")
            target = TargetRef.parse(args[0]) if args else None
            rows = self.db.list_schedules(
                target_id=target.telegram_id if target else None,
                target_topic_id=target.topic_id if target else 0,
            )
            return "发布时间：\n" + "\n".join(
                f"#{row['id']} {'✅' if row['enabled'] else '⏸'} "
                f"{target_display(row)} 每天 {row['time_hhmm']}"
                for row in rows
            ) if rows else "尚无发布时间。"
        if command == "/threshold":
            self._require(args, 2)
            target = TargetRef.parse(args[0])
            found = self.db.set_threshold(
                target.telegram_id, int(args[1]), target.topic_id
            )
            return "库存阈值已更新。" if found else "未找到目标频道。"
        if command in {"/alert_enable", "/alert_disable"}:
            self._require(args, 1)
            enabled = command.endswith("enable")
            target = TargetRef.parse(args[0])
            found = self.db.set_alert_enabled(
                target.telegram_id, enabled, target.topic_id
            )
            return "库存告警状态已更新。" if found else "未找到目标频道。"
        if command == "/alert_time":
            self._require(args, 1)
            hhmm = validate_hhmm(args[0])
            self.db.set_setting("alert_time", hhmm)
            return f"每日库存检查时间已改为 {hhmm}（Asia/Shanghai）。"
        if command == "/alert_status":
            return self._format_inventory(self.db.inventory(), include_alert=True)
        if command == "/queue":
            if len(args) > 1:
                raise ValueError("格式: /queue [目标标识]")
            target = TargetRef.parse(args[0]) if args else None
            rows = self.db.inventory(
                target.telegram_id if target else None,
                target.topic_id if target else 0,
            )
            return self._format_inventory(rows, include_alert=False)
        if command == "/status":
            counts = self.db.status_counts()
            details = "，".join(f"{key}={value}" for key, value in sorted(counts.items())) or "暂无投递"
            return (
                f"服务状态\n源频道：{len(self.db.list_sources(enabled_only=True))} 个启用\n"
                f"目标映射：{len(self.db.list_routes(enabled_only=True))} 个启用\n投递：{details}\n"
                f"库存检查：每天 {self.db.get_setting('alert_time', '09:00')}"
            )
        if command == "/issues":
            if len(args) > 1:
                raise ValueError("格式: /issues [目标标识]")
            target = TargetRef.parse(args[0]) if args else None
            rows = self.db.list_issues(
                target.telegram_id if target else None,
                target_topic_id=target.topic_id if target else 0,
            )
            if not rows:
                return "没有 failed 或 ambiguous 投递。"
            return "待人工处理：\n" + "\n".join(
                f"#{row['id']} {row['status']} | {target_display(row)} "
                f"| grouped_id={row['grouped_id']} | {row['last_error'] or '无错误详情'}"
                for row in rows
            )
        if command == "/retry":
            self._require(args, 1)
            return "投递已重新入队。" if self.db.retry_delivery(int(args[0])) else "该投递不存在或不可重试。"
        if command == "/skip":
            self._require(args, 1)
            return "投递已跳过。" if self.db.skip_delivery(int(args[0])) else "该投递不存在或不可跳过。"
        return "未知命令。发送 /help 查看帮助。"

    async def send_inventory_alerts(self, low_rows: list[Any], day: date) -> None:
        lines = [f"⚠️ 影视库低库存提醒（{day.isoformat()}）", ""]
        for row in low_rows:
            lines.append(
                f"• {target_display(row)}\n"
                f"  源：{row['source_title']} ({row['source_telegram_id']})\n"
                f"  剩余：{row['pending_count']} 组 / 阈值：{row['threshold']} 组\n"
                f"  每日发布时间：{row['schedule_times'] or '未配置'}"
            )
        text = "\n".join(lines)
        for admin_id in self.admin_ids:
            for attempt, delay in enumerate((0, 5, 15), start=1):
                if delay:
                    await asyncio.sleep(delay)
                try:
                    await self.client.send_message(admin_id, text)
                    break
                except Exception:
                    logger.exception("库存提醒发送失败 admin=%s attempt=%s", admin_id, attempt)

    async def send_scan_notice(self, admin_id: int, text: str) -> None:
        try:
            await self.client.send_message(admin_id, text)
        except Exception:
            logger.exception("历史扫描结果通知失败 admin=%s", admin_id)

    async def _verify_can_post(self, entity: Any) -> None:
        if getattr(entity, "creator", False):
            return
        rights = getattr(entity, "admin_rights", None)
        if getattr(entity, "broadcast", False):
            if rights and getattr(rights, "post_messages", False):
                return
            raise ValueError("用户账号不是该目标频道的可发帖管理员")
        permissions = await self.user_client.get_permissions(entity, "me")
        if not getattr(permissions, "send_messages", False):
            raise ValueError("用户账号没有向该目标发送消息的权限")

    async def _verify_forum_topic(self, entity: Any, topic_id: int) -> str:
        if topic_id <= 0:
            raise ValueError("话题 ID 必须是大于 0 的整数")
        if not getattr(entity, "forum", False):
            raise ValueError("该目标不是已开启话题功能的超级群组")
        result = await self.user_client(
            functions.messages.GetForumTopicsByIDRequest(
                peer=entity,
                topics=[topic_id],
            )
        )
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
            raise ValueError(f"未找到话题 ID {topic_id}，话题可能已删除")
        if getattr(topic, "closed", False):
            raise ValueError(f"话题 ID {topic_id} 已关闭")
        if getattr(topic, "hidden", False):
            raise ValueError(f"话题 ID {topic_id} 已隐藏")
        return str(topic.title)

    @staticmethod
    def _format_inventory(rows: list[Any], include_alert: bool) -> str:
        if not rows:
            return "未找到目标频道。"
        lines = ["目标库存："]
        for row in rows:
            suffix = ""
            if include_alert:
                suffix = f"，阈值={row['threshold']}，告警={'开' if row['alert_enabled'] else '关'}"
            lines.append(
                f"• {target_display(row)}: pending={row['pending_count']}{suffix}"
            )
        return "\n".join(lines)

    @staticmethod
    async def _reply_chunks(event: Any, response: BotResponse) -> None:
        messages = [response] if isinstance(response, str) else response
        first = True
        for message in messages:
            for chunk in ManagerBot._split_message(message):
                if first:
                    await event.reply(chunk)
                    first = False
                else:
                    await event.respond(chunk)

    @staticmethod
    def _split_message(text: str, limit: int = 4000) -> list[str]:
        chunks: list[str] = []
        remaining = text.strip()
        while len(remaining) > limit:
            split_at = remaining.rfind("\n\n", 0, limit + 1)
            if split_at <= 0:
                split_at = remaining.rfind("\n", 0, limit + 1)
            if split_at <= 0:
                split_at = limit
            chunks.append(remaining[:split_at].rstrip())
            remaining = remaining[split_at:].lstrip()
        if remaining:
            chunks.append(remaining)
        return chunks

    @staticmethod
    def _require(args: list[str], count: int) -> None:
        if len(args) != count:
            raise ValueError("命令参数数量不正确，请发送 /help 查看格式")

    @staticmethod
    def _require_at_least(args: list[str], count: int) -> None:
        if len(args) < count:
            raise ValueError("命令参数数量不正确，请发送 /help 查看格式")

    @staticmethod
    def _require_range(args: list[str], minimum: int, maximum: int) -> None:
        if not minimum <= len(args) <= maximum:
            raise ValueError("命令参数数量不正确，请发送 /help 查看格式")

    @staticmethod
    def _validated_times(values: list[str]) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            hhmm = validate_hhmm(value)
            if hhmm not in seen:
                seen.add(hhmm)
                result.append(hhmm)
        return result

    @staticmethod
    def _history_start(value: str | None, latest_id: int, label: str) -> int | None:
        if value is None:
            return None
        if value.lower() == "all":
            return 1
        try:
            start_id = int(value)
        except ValueError as exc:
            raise ValueError(f"{label}范围必须是 all 或正整数消息 ID") from exc
        if start_id <= 0:
            raise ValueError(f"{label}起点消息 ID 必须大于 0")
        if start_id > latest_id:
            raise ValueError(f"{label}起点不能超过源频道当前最新消息 ID {latest_id}")
        return start_id

    @staticmethod
    def _validate_command_specs() -> None:
        names: set[str] = set()
        for spec in BOT_COMMAND_SPECS:
            if not re.fullmatch(r"[a-z0-9_]{1,32}", spec.name):
                raise ValueError(f"无效机器人菜单命令名: {spec.name}")
            if spec.name in names:
                raise ValueError(f"重复机器人菜单命令名: {spec.name}")
            if not spec.description or len(spec.description) > 256:
                raise ValueError(f"无效机器人菜单描述: {spec.name}")
            names.add(spec.name)

    @staticmethod
    def _peer(value: str) -> int | str:
        try:
            return int(value)
        except ValueError:
            return value

    @staticmethod
    def _title(entity: Any) -> str:
        return str(getattr(entity, "title", None) or getattr(entity, "username", None) or entity.id)
