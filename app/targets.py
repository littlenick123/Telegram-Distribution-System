from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any


_TARGET_RE = re.compile(r"^(?P<chat>-?\d+)(?::(?P<topic>\d+))?$")


@dataclass(frozen=True, slots=True)
class TargetRef:
    telegram_id: int
    topic_id: int = 0

    @classmethod
    def parse(cls, value: str) -> "TargetRef":
        match = _TARGET_RE.fullmatch(value.strip())
        if not match:
            raise ValueError("目标格式错误，应为 <频道ID> 或 <群组ID>:<话题ID>")
        telegram_id = int(match.group("chat"))
        topic_id = int(match.group("topic") or 0)
        if telegram_id >= 0:
            raise ValueError("目标频道或群组 ID 必须是负数")
        if match.group("topic") is not None and topic_id <= 0:
            raise ValueError("话题 ID 必须是大于 0 的整数")
        return cls(telegram_id, topic_id)

    def __str__(self) -> str:
        if self.topic_id:
            return f"{self.telegram_id}:{self.topic_id}"
        return str(self.telegram_id)


def target_ref_from_row(row: Any) -> TargetRef:
    return TargetRef(int(row["target_telegram_id"]), int(row["target_topic_id"] or 0))


def target_display(row: Any) -> str:
    title = str(row["target_title"])
    topic_title = row["target_topic_title"]
    if topic_title:
        title = f"{title} / {topic_title}"
    return f"{title} ({target_ref_from_row(row)})"
