from __future__ import annotations

from pathlib import Path
import re
import unittest

from app.manager_bot import BOT_COMMAND_SPECS, HELP_SECTIONS, HELP_TEXT, WELCOME_TEXT


class DocumentationTests(unittest.TestCase):
    def test_readme_documents_every_bot_command(self) -> None:
        readme = (Path(__file__).parents[1] / "README.md").read_text(encoding="utf-8")
        commands = set(re.findall(r"^(/[a-z_]+)", HELP_TEXT, flags=re.MULTILINE))
        missing = sorted(command for command in commands if f"#### `{command}" not in readme)
        self.assertEqual(missing, [], f"README 缺少机器人命令说明: {missing}")

    def test_help_sections_fit_telegram_limit(self) -> None:
        self.assertEqual(len(HELP_SECTIONS), 5)
        self.assertLessEqual(len(WELCOME_TEXT), 4000)
        self.assertTrue(all(len(section) <= 4000 for section in HELP_SECTIONS))

    def test_command_menu_metadata_is_valid_and_matches_help(self) -> None:
        names = [spec.name for spec in BOT_COMMAND_SPECS]
        self.assertEqual(len(names), len(set(names)))
        self.assertTrue(all(re.fullmatch(r"[a-z0-9_]{1,32}", name) for name in names))
        self.assertTrue(all(0 < len(spec.description) <= 256 for spec in BOT_COMMAND_SPECS))
        help_names = set(re.findall(r"^/([a-z_]+)", HELP_TEXT, flags=re.MULTILINE))
        self.assertEqual(set(names), help_names)


if __name__ == "__main__":
    unittest.main()
