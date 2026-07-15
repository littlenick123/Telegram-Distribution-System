from __future__ import annotations

import argparse
import asyncio
import logging

from .config import Config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Telegram 私密影视库定时分发系统")
    parser.add_argument("command", choices=("login", "run", "init-db"))
    parser.add_argument("--env-file", default=".env", help="环境配置文件路径")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        config = Config.from_env(args.env_file)
    except ValueError as exc:
        raise SystemExit(f"配置错误：{exc}") from exc
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if args.command == "init-db":
        from .db import Database

        db = Database(config.database_path)
        db.initialize()
        db.close()
        print(f"数据库已初始化：{config.database_path}")
        return

    from .service import login, run_service

    try:
        asyncio.run(login(config) if args.command == "login" else run_service(config))
    except KeyboardInterrupt:
        print("服务已停止。")


if __name__ == "__main__":
    main()
