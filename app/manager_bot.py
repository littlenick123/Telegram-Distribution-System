from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
import logging
import re
import shlex
from typing import Any, Iterable

from telethon import TelegramClient, events, functions, types

from .db import Database
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
    BotCommandSpec("source_add", "添加源频道（不导入旧帖）", 1, "/source_add <源频道ID>", "/source_add -1001234567890"),
    BotCommandSpec("source_list", "查看所有源频道", 0, "/source_list", "/source_list"),
    BotCommandSpec("source_enable", "启用源频道", 1, "/source_enable <源频道ID>", "/source_enable -1001234567890"),
    BotCommandSpec("source_disable", "停用源频道", 1, "/source_disable <源频道ID>", "/source_disable -1001234567890"),
    BotCommandSpec("route_add", "添加源到目标频道映射", 2, "/route_add <源频道ID> <目标频道ID>", "/route_add -1001234567890 -1009876543210"),
    BotCommandSpec("route_list", "查看全部频道映射", 0, "/route_list", "/route_list"),
    BotCommandSpec("route_enable", "启用目标频道映射", 1, "/route_enable <目标频道ID>", "/route_enable -1009876543210"),
    BotCommandSpec("route_disable", "停用目标频道映射", 1, "/route_disable <目标频道ID>", "/route_disable -1009876543210"),
    BotCommandSpec("route_backfill", "补充目标的现有媒体库存", 2, "/route_backfill <目标频道ID> all", "/route_backfill -1009876543210 all"),
    BotCommandSpec("schedule_add", "批量添加每日发布时间", 2, "/schedule_add <目标频道ID> <HH:MM> [HH:MM ...]", "/schedule_add -1009876543210 09:00 13:00 20:00"),
    BotCommandSpec("schedule_list", "查看全部或目标发布时间", 0, "/schedule_list [目标频道ID]", "/schedule_list -1009876543210"),
    BotCommandSpec("schedule_del", "删除一个或多个发布时间", 1, "/schedule_del <计划ID> 或 <目标频道ID> <HH:MM|all>", "/schedule_del -1009876543210 all"),
    BotCommandSpec("threshold", "设置目标低库存阈值", 2, "/threshold <目标频道ID> <数量>", "/threshold -1009876543210 24"),
    BotCommandSpec("alert_enable", "开启目标低库存提醒", 1, "/alert_enable <目标频道ID>", "/alert_enable -1009876543210"),
    BotCommandSpec("alert_disable", "关闭目标低库存提醒", 1, "/alert_disable <目标频道ID>", "/alert_disable -1009876543210"),
    BotCommandSpec("alert_time", "设置每日库存检查时间", 1, "/alert_time <HH:MM>", "/alert_time 09:00"),
    BotCommandSpec("alert_status", "查看全部目标告警状态", 0, "/alert_status", "/alert_status"),
    BotCommandSpec("queue", "查看全部或目标待发库存", 0, "/queue [目标频道ID]", "/queue -1009876543210"),
    BotCommandSpec("status", "查看服务和投递状态", 0, "/status", "/status"),
    BotCommandSpec("issues", "查看失败或状态不明投递", 0, "/issues [目标频道ID]", "/issues -1009876543210"),
    BotCommandSpec("retry", "重新入队失败或不明投递", 1, "/retry <投递ID>", "/retry 42"),
    BotCommandSpec("skip", "取消指定投递", 1, "/skip <投递ID>", "/skip 42"),
)

BOT_COMMANDS_BY_NAME = {f"/{spec.name}": spec for spec in BOT_COMMAND_SPECS}


WELCOME_TEXT = """🎬 欢迎使用影视库管理机器人

本机器人用于管理私密影视库的采集路由、定时发布、库存提醒和异常任务。

首次配置顺序：
1. /source_add -100源频道ID
2. /route_add -100源频道ID -100目标频道ID
3. /schedule_add -100目标频道ID 09:00 13:00 20:00
4. /threshold -100目标频道ID 24
5. /status 检查服务状态

下面将继续发送完整的分类命令说明。"""


HELP_SECTIONS = (
    """📖 使用规则

• 只能在机器人私聊中使用，且发送者必须在 ADMIN_USER_IDS 白名单中。
• <参数> 为必填，[参数] 为可选；输入时不要带尖括号或方括号。
• 频道 ID 通常以 -100 开头，例如 -1001234567890。
• 时间使用 24 小时制 HH:MM，统一按 Asia/Shanghai 解释。
• 系统只采集至少包含两项媒体的媒体组；单媒体和纯文本会忽略。
• /start 显示首次配置引导和完整帮助；/help 随时重新显示完整帮助。""",
    """📥 源频道与路由

/source_add <源频道ID>
添加源频道并以当前最新消息为水位，不导入旧帖。
示例：/source_add -1001234567890

/source_list
列出源频道、启停状态和采集水位。
示例：/source_list

/source_enable <源频道ID>
重新启用源频道；停用期间的新组可能从原水位补录。
示例：/source_enable -1001234567890

/source_disable <源频道ID>
暂停源频道采集及其目标发布，不删除数据库内容。
示例：/source_disable -1001234567890

/route_add <源频道ID> <目标频道ID>
建立一源到一目标的投递映射，只让后续新组进入目标队列。
示例：/route_add -1001234567890 -1009876543210

/route_list
列出全部源到目标映射。
示例：/route_list

/route_enable <目标频道ID>
启用目标映射并继续已有 pending 队列。
示例：/route_enable -1009876543210

/route_disable <目标频道ID>
暂停目标发布及新投递创建，不删除已有记录。
示例：/route_disable -1009876543210

/route_backfill <目标频道ID> all
把该源在数据库中的全部有效媒体组补入目标队列。
示例：/route_backfill -1009876543210 all
⚠️ 包括已在其他目标发布的内容；重复执行不会重复入队。""",
    """⏰ 发布时间管理

/schedule_add <目标频道ID> <HH:MM> [HH:MM ...]
一次添加一个或多个每日发布时间。
示例：/schedule_add -1009876543210 09:00 13:00 20:00

/schedule_list [目标频道ID]
不带参数查看全部计划，带目标 ID 只查看该目标。
示例：/schedule_list -1009876543210

/schedule_del <计划ID>
按 schedule_list 显示的正数计划 ID 删除一条计划。
示例：/schedule_del 3

/schedule_del <目标频道ID> <HH:MM> [HH:MM ...]
批量删除该目标的指定发布时间。
示例：/schedule_del -1009876543210 09:00 13:00

/schedule_del <目标频道ID> all
删除该目标的全部发布时间。
示例：/schedule_del -1009876543210 all
⚠️ all 会立即删除该目标全部计划，但不会删除待发库存。""",
    """📦 库存与提醒

/threshold <目标频道ID> <数量>
设置目标低库存阈值，必须大于 0；默认 24。
示例：/threshold -1009876543210 24

/alert_enable <目标频道ID>
开启目标的每日低库存提醒。
示例：/alert_enable -1009876543210

/alert_disable <目标频道ID>
关闭目标提醒，不影响正常采集和发布。
示例：/alert_disable -1009876543210

/alert_time <HH:MM>
设置全局每日库存检查时间。
示例：/alert_time 09:00

/alert_status
查看所有目标的 pending 数量、阈值和提醒开关。
示例：/alert_status

/queue [目标频道ID]
查看全部或指定目标的可发布 pending 库存。
示例：/queue -1009876543210""",
    """🛠 状态与故障处理

/status
查看启用源、目标映射、各投递状态数量和提醒时间。
示例：/status

/issues [目标频道ID]
列出最近的 failed 或 ambiguous 投递及投递 ID。
示例：/issues -1009876543210

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
    ):
        self.client = bot_client
        self.user_client = user_client
        self.db = db
        self.admin_ids = frozenset(int(value) for value in admin_ids)

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
                response = await self.dispatch(command, args)
        except Exception as exc:
            logger.exception("管理命令执行失败 command=%s", event.raw_text)
            response = f"操作失败：{exc}"
        await self._reply_chunks(event, response)

    async def dispatch(self, command: str, args: list[str]) -> BotResponse:
        if command == "/start":
            return [WELCOME_TEXT, *HELP_SECTIONS]
        if command == "/help":
            return list(HELP_SECTIONS)
        if command == "/source_add":
            self._require(args, 1)
            entity = await self.user_client.get_entity(self._peer(args[0]))
            peer_id = canonical_peer_id(entity)
            latest = await self.user_client.get_messages(entity, limit=1)
            watermark = int(latest[0].id) if latest else 0
            self.db.add_source(peer_id, self._title(entity), watermark)
            return f"已添加源频道：{self._title(entity)} ({peer_id})，水位 {watermark}，旧帖不会入库。"
        if command == "/source_list":
            rows = self.db.list_sources()
            return "源频道：\n" + "\n".join(
                f"{'✅' if row['enabled'] else '⏸'} {row['title']} ({row['telegram_id']}) 水位={row['last_message_id']}"
                for row in rows
            ) if rows else "尚无源频道。"
        if command in {"/source_enable", "/source_disable"}:
            self._require(args, 1)
            enabled = command.endswith("enable")
            found = self.db.set_source_enabled(int(args[0]), enabled)
            return "源频道状态已更新。" if found else "未找到源频道。"
        if command == "/route_add":
            self._require(args, 2)
            source_id = int(args[0])
            if not self.db.get_source(source_id):
                raise ValueError("请先添加源频道")
            target = await self.user_client.get_entity(self._peer(args[1]))
            await self._verify_can_post(target)
            target_id = canonical_peer_id(target)
            self.db.add_route(source_id, target_id, self._title(target))
            return f"已建立映射：{source_id} → {self._title(target)} ({target_id})。仅后续新媒体组会进入该队列。"
        if command == "/route_list":
            rows = self.db.list_routes()
            return "频道映射：\n" + "\n".join(
                f"{'✅' if row['enabled'] else '⏸'} {row['source_title']} ({row['source_telegram_id']}) → "
                f"{row['target_title']} ({row['target_telegram_id']})"
                for row in rows
            ) if rows else "尚无频道映射。"
        if command in {"/route_enable", "/route_disable"}:
            self._require(args, 1)
            enabled = command.endswith("enable")
            found = self.db.set_route_enabled(int(args[0]), enabled)
            return "映射状态已更新。" if found else "未找到目标频道。"
        if command == "/route_backfill":
            self._require(args, 2)
            if args[1].lower() != "all":
                raise ValueError("当前仅支持: /route_backfill <目标频道ID> all")
            target_id = int(args[0])
            created, existing, pending = self.db.backfill_route(target_id)
            return (
                f"库存回填完成：新增 {created} 组，已存在 {existing} 组，"
                f"目标当前 pending={pending}。"
            )
        if command == "/schedule_add":
            self._require_at_least(args, 2)
            target_id = int(args[0])
            times = self._validated_times(args[1:])
            now = utc_now()
            results = self.db.add_schedules(
                target_id,
                [(hhmm, initial_schedule_date(hhmm, now)) for hhmm in times],
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
            identifier = int(args[0])
            if identifier >= 0:
                self._require(args, 1)
                return "发布时间已删除。" if self.db.delete_schedule(identifier) else "未找到计划。"
            if len(args) == 1:
                raise ValueError(
                    "按目标删除时必须提供 HH:MM 或 all，例如: "
                    "/schedule_del -1001234567890 all"
                )
            if args[1].lower() == "all":
                self._require(args, 2)
                deleted, missing = self.db.delete_schedules_by_target(identifier, None)
            else:
                times = self._validated_times(args[1:])
                deleted, missing = self.db.delete_schedules_by_target(identifier, times)
            lines = [f"发布时间删除完成：删除 {len(deleted)} 个，未找到 {len(missing)} 个。"]
            if deleted:
                lines.append("已删除：" + "，".join(deleted))
            if missing:
                lines.append("未找到：" + "，".join(missing))
            return "\n".join(lines)
        if command == "/schedule_list":
            if len(args) > 1:
                raise ValueError("格式: /schedule_list [目标频道ID]")
            target_id = int(args[0]) if args else None
            rows = self.db.list_schedules(target_id=target_id)
            return "发布时间：\n" + "\n".join(
                f"#{row['id']} {'✅' if row['enabled'] else '⏸'} {row['target_title']} "
                f"({row['target_telegram_id']}) 每天 {row['time_hhmm']}"
                for row in rows
            ) if rows else "尚无发布时间。"
        if command == "/threshold":
            self._require(args, 2)
            found = self.db.set_threshold(int(args[0]), int(args[1]))
            return "库存阈值已更新。" if found else "未找到目标频道。"
        if command in {"/alert_enable", "/alert_disable"}:
            self._require(args, 1)
            enabled = command.endswith("enable")
            found = self.db.set_alert_enabled(int(args[0]), enabled)
            return "库存告警状态已更新。" if found else "未找到目标频道。"
        if command == "/alert_time":
            self._require(args, 1)
            hhmm = validate_hhmm(args[0])
            self.db.set_setting("alert_time", hhmm)
            return f"每日库存检查时间已改为 {hhmm}（Asia/Shanghai）。"
        if command == "/alert_status":
            return self._format_inventory(self.db.inventory(), include_alert=True)
        if command == "/queue":
            target_id = int(args[0]) if args else None
            return self._format_inventory(self.db.inventory(target_id), include_alert=False)
        if command == "/status":
            counts = self.db.status_counts()
            details = "，".join(f"{key}={value}" for key, value in sorted(counts.items())) or "暂无投递"
            return (
                f"服务状态\n源频道：{len(self.db.list_sources(enabled_only=True))} 个启用\n"
                f"目标映射：{len(self.db.list_routes(enabled_only=True))} 个启用\n投递：{details}\n"
                f"库存检查：每天 {self.db.get_setting('alert_time', '09:00')}"
            )
        if command == "/issues":
            target_id = int(args[0]) if args else None
            rows = self.db.list_issues(target_id)
            if not rows:
                return "没有 failed 或 ambiguous 投递。"
            return "待人工处理：\n" + "\n".join(
                f"#{row['id']} {row['status']} | {row['target_title']} ({row['target_telegram_id']}) "
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
                f"• {row['target_title']} ({row['target_telegram_id']})\n"
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
                f"• {row['target_title']} ({row['target_telegram_id']}): pending={row['pending_count']}{suffix}"
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
