from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


def _load_dotenv(path: Path) -> None:
    """Load a small, predictable subset of dotenv syntax."""
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key, value)


@dataclass(frozen=True, slots=True)
class Config:
    api_id: int
    api_hash: str
    phone: str
    bot_token: str
    admin_user_ids: frozenset[int]
    data_dir: Path
    log_level: str = "INFO"

    @property
    def user_session(self) -> Path:
        return self.data_dir / "user"

    @property
    def bot_session(self) -> Path:
        return self.data_dir / "manager_bot"

    @property
    def database_path(self) -> Path:
        return self.data_dir / "library.sqlite3"

    @classmethod
    def from_env(cls, env_file: str | Path = ".env") -> "Config":
        _load_dotenv(Path(env_file))
        missing = [
            name
            for name in ("TG_API_ID", "TG_API_HASH", "TG_PHONE", "MANAGER_BOT_TOKEN", "ADMIN_USER_IDS")
            if not os.getenv(name)
        ]
        if missing:
            raise ValueError("缺少环境变量: " + ", ".join(missing))
        try:
            api_id = int(os.environ["TG_API_ID"])
            admins = frozenset(
                int(value.strip())
                for value in os.environ["ADMIN_USER_IDS"].split(",")
                if value.strip()
            )
        except ValueError as exc:
            raise ValueError("TG_API_ID 和 ADMIN_USER_IDS 必须是整数") from exc
        if not admins:
            raise ValueError("ADMIN_USER_IDS 至少需要一个 Telegram 用户 ID")
        data_dir = Path(os.getenv("DATA_DIR", "data")).expanduser().resolve()
        data_dir.mkdir(parents=True, exist_ok=True)
        return cls(
            api_id=api_id,
            api_hash=os.environ["TG_API_HASH"],
            phone=os.environ["TG_PHONE"],
            bot_token=os.environ["MANAGER_BOT_TOKEN"],
            admin_user_ids=admins,
            data_dir=data_dir,
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        )

