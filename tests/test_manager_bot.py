from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from telethon.tl import functions, types

from app.db import Database
from app.manager_bot import BOT_COMMAND_SPECS, HELP_SECTIONS, WELCOME_TEXT, ManagerBot
from app.targets import TargetRef
from app.telegram_service import canonical_peer_id


class FakeEvent:
    def __init__(self, raw_text: str = "") -> None:
        self.replies: list[str] = []
        self.responses: list[str] = []
        self.raw_text = raw_text
        self.is_private = True
        self.sender_id = 123

    async def reply(self, text: str) -> None:
        self.replies.append(text)

    async def respond(self, text: str) -> None:
        self.responses.append(text)


class FakeMenuClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.requests: list[object] = []

    async def __call__(self, request: object) -> bool:
        self.requests.append(request)
        if self.fail:
            raise RuntimeError("menu sync failed")
        return True


class FakeTopicUserClient:
    def __init__(
        self,
        *,
        forum: bool = True,
        topics: list[object] | None = None,
        megagroup: bool = True,
        broadcast: bool = False,
    ) -> None:
        self.entity = types.Channel(
            id=2001,
            title="影视讨论群",
            photo=types.ChatPhotoEmpty(),
            date=None,
            creator=True,
            megagroup=megagroup,
            broadcast=broadcast,
            forum=forum,
        )
        self.topics = topics if topics is not None else [
            SimpleNamespace(id=135, title="动作电影", closed=False, hidden=False)
        ]
        self.requests: list[object] = []

    async def get_entity(self, value: object) -> object:
        return self.entity

    async def get_messages(self, entity: object, **kwargs: object) -> list[object]:
        return [SimpleNamespace(id=5000)]

    async def __call__(self, request: object) -> object:
        self.requests.append(request)
        return SimpleNamespace(topics=self.topics)


class ManagerBotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "test.sqlite3")
        self.db.initialize()
        self.db.add_source(-1001, "源频道", 10)
        self.db.add_route(-1001, -2001, "目标频道")
        user_client = SimpleNamespace()

        async def get_messages(entity: object, **kwargs: object) -> list[object]:
            return [SimpleNamespace(id=100)]

        user_client.get_messages = get_messages
        self.bot = ManagerBot(None, user_client, self.db, [123])

    async def asyncTearDown(self) -> None:
        self.db.close()
        self.tempdir.cleanup()

    async def test_batch_schedule_commands_and_delete_protection(self) -> None:
        response = await self.bot.dispatch(
            "/schedule_add", ["-2001", "09:00", "13:00", "09:00"]
        )
        self.assertIn("新增 2 个", response)
        self.assertEqual(len(self.db.list_schedules(target_id=-2001)), 2)

        with self.assertRaises(ValueError):
            await self.bot.dispatch("/schedule_add", ["-2001", "18:00", "25:00"])
        self.assertEqual(len(self.db.list_schedules(target_id=-2001)), 2)

        listed = await self.bot.dispatch("/schedule_list", ["-2001"])
        self.assertIn("09:00", listed)
        with self.assertRaisesRegex(ValueError, "必须提供"):
            await self.bot.dispatch("/schedule_del", ["-2001"])

        deleted = await self.bot.dispatch("/schedule_del", ["-2001", "09:00"])
        self.assertIn("删除 1 个", deleted)
        await self.bot.dispatch("/schedule_del", ["-2001", "all"])
        self.assertEqual(self.db.list_schedules(target_id=-2001), [])

    async def test_old_schedule_id_delete_still_works(self) -> None:
        await self.bot.dispatch("/schedule_add", ["-2001", "09:00"])
        slot_id = self.db.list_schedules(target_id=-2001)[0]["id"]
        response = await self.bot.dispatch("/schedule_del", [str(slot_id)])
        self.assertEqual(response, "发布时间已删除。")

    async def test_route_backfill_command_is_idempotent(self) -> None:
        self.db.ingest_album(
            -1001,
            500,
            [11, 12],
            datetime(2026, 7, 15, tzinfo=timezone.utc),
        )
        first = await self.bot.dispatch("/route_backfill", ["-2001", "all"])
        second = await self.bot.dispatch("/route_backfill", ["-2001", "all"])
        self.assertIn("新增 0 组", first)  # Normal ingestion already created this route's delivery.
        self.assertIn("新增 0 组", second)
        with self.assertRaises(ValueError):
            await self.bot.dispatch("/route_backfill", ["-2001", "pending"])

    async def test_source_add_accepts_history_message_id(self) -> None:
        wakeups: list[bool] = []
        user_client = FakeTopicUserClient()
        bot = ManagerBot(
            None,
            user_client,
            self.db,
            [123],
            history_wakeup=lambda: wakeups.append(True),
        )
        response = await bot.dispatch(
            "/source_add", ["-1000000002001", "4348"], requester_id=123
        )
        source_id = canonical_peer_id(user_client.entity)
        source = self.db.get_source(source_id)
        self.assertEqual(source["history_requested_min_id"], 4348)
        self.assertEqual(source["history_scan_status"], "pending")
        self.assertEqual(source["history_requested_by"], 123)
        self.assertEqual(wakeups, [True])
        self.assertIn("消息 4348 起", response)

    async def test_topic_route_backfill_accepts_message_id(self) -> None:
        self.db.add_route(
            -1001,
            -2001,
            "话题群",
            135,
            "动作电影",
            delivery_start_message_id=101,
        )
        response = await self.bot.dispatch(
            "/route_backfill", ["-2001:135", "50"], requester_id=123
        )
        route = self.db.get_route_by_target(-2001, 135)
        self.assertEqual(route["delivery_start_message_id"], 50)
        self.assertEqual(route["backfill_status"], "pending")
        self.assertIn("独立起点=50", response)

    async def test_start_and_help_return_categorized_messages(self) -> None:
        start = await self.bot.dispatch("/start", [])
        help_response = await self.bot.dispatch("/help", [])
        self.assertEqual(start, [WELCOME_TEXT, *HELP_SECTIONS])
        self.assertEqual(help_response, list(HELP_SECTIONS))
        self.assertEqual(len(help_response), 5)
        self.assertTrue(all(len(section) <= 4000 for section in start))
        self.assertIn("/route_backfill", "\n".join(help_response))
        self.assertIn("/route_del", "\n".join(help_response))
        self.assertIn("/schedule_del <目标标识> all", "\n".join(help_response))

    async def test_topic_target_commands_use_composite_reference(self) -> None:
        self.db.add_route(-1001, -2001, "话题群", 135, "动作电影")
        added = await self.bot.dispatch(
            "/schedule_add", ["-2001:135", "09:00", "18:00"]
        )
        self.assertIn("新增 2 个", added)
        self.assertEqual(
            len(self.db.list_schedules(target_id=-2001, target_topic_id=135)), 2
        )
        status = await self.bot.dispatch("/alert_status", [])
        self.assertIn("话题群 / 动作电影 (-2001:135)", status)
        await self.bot.dispatch("/route_disable", ["-2001:135"])
        self.assertFalse(self.db.get_route_by_target(-2001, 135)["enabled"])
        await self.bot.dispatch("/schedule_del", ["-2001:135", "all"])
        self.assertEqual(
            self.db.list_schedules(target_id=-2001, target_topic_id=135), []
        )

    async def test_route_del_removes_topic_route_and_reports_missing_target(self) -> None:
        self.db.add_route(-1001, -2001, "话题群", 135, "动作电影")
        self.db.ingest_album(
            -1001,
            500,
            [11, 12],
            datetime(2026, 7, 15, tzinfo=timezone.utc),
        )
        await self.bot.dispatch("/schedule_add", ["-2001:135", "09:00"])

        response = await self.bot.dispatch("/route_del", ["-2001:135"])

        self.assertIn("已永久删除目标映射", response)
        self.assertIn("pending=1", response)
        self.assertIn("发布时间=1", response)
        self.assertIsNone(self.db.get_route_by_target(-2001, 135))
        missing = await self.bot.dispatch("/route_del", ["-2001:135"])
        self.assertEqual(missing, "未找到目标频道，未删除任何数据。")

    async def test_route_add_validates_and_saves_forum_topic(self) -> None:
        user_client = FakeTopicUserClient()
        bot = ManagerBot(None, user_client, self.db, [123])
        response = await bot.dispatch(
            "/route_add", ["-1001", "-1000000002001", "135"]
        )
        target_id = canonical_peer_id(user_client.entity)
        route = self.db.get_route_by_target(target_id, 135)
        self.assertIsNotNone(route)
        self.assertEqual(route["target_topic_title"], "动作电影")
        self.assertIn(f"{target_id}:135", response)
        self.assertIsInstance(
            user_client.requests[0], functions.messages.GetForumTopicsByIDRequest
        )

        non_forum = FakeTopicUserClient(forum=False)
        with self.assertRaisesRegex(ValueError, "开启话题"):
            await ManagerBot(None, non_forum, self.db, [123]).dispatch(
                "/route_add", ["-1001", "-1000000002001", "246"]
            )
        missing = FakeTopicUserClient(topics=[])
        with self.assertRaisesRegex(ValueError, "未找到话题"):
            await ManagerBot(None, missing, self.db, [123]).dispatch(
                "/route_add", ["-1001", "-1000000002001", "246"]
            )

    async def test_route_add_can_start_normal_target_backfill(self) -> None:
        wakeups: list[bool] = []
        user_client = FakeTopicUserClient(forum=False)
        bot = ManagerBot(
            None,
            user_client,
            self.db,
            [123],
            history_wakeup=lambda: wakeups.append(True),
        )
        response = await bot.dispatch(
            "/route_add",
            ["-1001", "-1000000002001", "all"],
            requester_id=123,
        )
        target_id = canonical_peer_id(user_client.entity)
        route = self.db.get_route_by_target(target_id)
        self.assertEqual(route["delivery_start_message_id"], 1)
        self.assertEqual(route["backfill_status"], "pending")
        self.assertEqual(route["backfill_requested_by"], 123)
        self.assertEqual(wakeups, [True])
        self.assertIn("全部历史", response)

    async def test_route_add_accepts_message_start_for_broadcast_channel(self) -> None:
        user_client = FakeTopicUserClient(
            forum=False,
            megagroup=False,
            broadcast=True,
        )
        bot = ManagerBot(None, user_client, self.db, [123])
        response = await bot.dispatch(
            "/route_add",
            ["-1001", "-1000000002001", "4348"],
            requester_id=123,
        )
        target_id = canonical_peer_id(user_client.entity)
        route = self.db.get_route_by_target(target_id)
        self.assertEqual(route["delivery_start_message_id"], 4348)
        self.assertIn("消息 4348 起", response)

    async def test_route_add_can_start_topic_backfill_with_composite_target(self) -> None:
        user_client = FakeTopicUserClient()
        bot = ManagerBot(None, user_client, self.db, [123])
        response = await bot.dispatch(
            "/route_add",
            ["-1001", "-1000000002001:135", "4348"],
            requester_id=123,
        )
        target_id = canonical_peer_id(user_client.entity)
        route = self.db.get_route_by_target(target_id, 135)
        self.assertEqual(route["delivery_start_message_id"], 4348)
        self.assertEqual(route["target_topic_title"], "动作电影")
        self.assertIn("消息 4348 起", response)

    async def test_target_reference_validation(self) -> None:
        self.assertEqual(TargetRef.parse("-2001"), TargetRef(-2001, 0))
        self.assertEqual(TargetRef.parse("-2001:135"), TargetRef(-2001, 135))
        for invalid in ("2001", "-2001:0", "-2001:-1", "-2001:abc"):
            with self.assertRaises(ValueError):
                TargetRef.parse(invalid)

    async def test_help_sections_are_sent_as_separate_messages(self) -> None:
        event = FakeEvent()
        await self.bot._reply_chunks(event, list(HELP_SECTIONS))
        self.assertEqual(event.replies, [HELP_SECTIONS[0]])
        self.assertEqual(event.responses, list(HELP_SECTIONS[1:]))

    async def test_syncs_all_commands_to_private_user_scope(self) -> None:
        client = FakeMenuClient()
        manager = ManagerBot(client, None, self.db, [123])
        self.assertTrue(await manager.sync_command_menu())
        self.assertEqual(len(client.requests), 1)
        request = client.requests[0]
        self.assertIsInstance(request, functions.bots.SetBotCommandsRequest)
        self.assertIsInstance(request.scope, types.BotCommandScopeUsers)
        self.assertEqual(request.lang_code, "")
        self.assertEqual(
            [command.command for command in request.commands],
            [spec.name for spec in BOT_COMMAND_SPECS],
        )

    async def test_menu_sync_failure_is_non_fatal(self) -> None:
        manager = ManagerBot(FakeMenuClient(fail=True), None, self.db, [123])
        self.assertFalse(await manager.sync_command_menu())

    async def test_bare_parameter_command_returns_specific_usage(self) -> None:
        event = FakeEvent("/source_add")
        await self.bot._handle_command(event)
        self.assertEqual(len(event.replies), 1)
        self.assertIn("格式：/source_add <源频道ID>", event.replies[0])
        self.assertIn("示例：/source_add -1001234567890", event.replies[0])
        self.assertEqual(event.responses, [])


if __name__ == "__main__":
    unittest.main()
