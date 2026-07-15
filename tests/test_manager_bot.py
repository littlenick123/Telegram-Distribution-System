from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest

from telethon.tl import functions, types

from app.db import Database
from app.manager_bot import BOT_COMMAND_SPECS, HELP_SECTIONS, WELCOME_TEXT, ManagerBot


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


class ManagerBotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tempdir.name) / "test.sqlite3")
        self.db.initialize()
        self.db.add_source(-1001, "源频道", 10)
        self.db.add_route(-1001, -2001, "目标频道")
        self.bot = ManagerBot(None, None, self.db, [123])

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

    async def test_start_and_help_return_categorized_messages(self) -> None:
        start = await self.bot.dispatch("/start", [])
        help_response = await self.bot.dispatch("/help", [])
        self.assertEqual(start, [WELCOME_TEXT, *HELP_SECTIONS])
        self.assertEqual(help_response, list(HELP_SECTIONS))
        self.assertEqual(len(help_response), 5)
        self.assertTrue(all(len(section) <= 4000 for section in start))
        self.assertIn("/route_backfill", "\n".join(help_response))
        self.assertIn("/schedule_del <目标频道ID> all", "\n".join(help_response))

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
