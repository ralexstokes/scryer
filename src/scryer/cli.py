from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from .config import load_config
from .daemon import DaemonService
from .db import Database
from .doctor import print_doctor_report, run_doctor
from .gh import GhClient
from .poller import Poller
from .pr import PRManager
from .runner import CodexRunner


def detect_repo_root() -> Path:
    proc = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return Path.cwd()
    return Path(proc.stdout.strip()).resolve()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scryer GitHub enhancement issue daemon")

    def add_common_args(target: argparse.ArgumentParser, *, with_defaults: bool) -> None:
        config_default = "config.toml" if with_defaults else argparse.SUPPRESS
        log_default = "INFO" if with_defaults else argparse.SUPPRESS
        target.add_argument("--config", default=config_default, help="Path to config TOML file")
        target.add_argument(
            "--log-level",
            default=log_default,
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            help="Logging level",
        )

    add_common_args(parser, with_defaults=True)

    sub = parser.add_subparsers(dest="command", required=True)
    add_common_args(
        sub.add_parser("status", help="Show issue status counts from SQLite"),
        with_defaults=False,
    )
    add_common_args(
        sub.add_parser("run-once", help="Run one poll/claim/execute cycle"),
        with_defaults=False,
    )
    add_common_args(
        sub.add_parser("daemon", help="Run the continuous daemon loop"),
        with_defaults=False,
    )
    add_common_args(
        sub.add_parser("doctor", help="Run environment and integration readiness checks"),
        with_defaults=False,
    )
    return parser


def build_service(config_path: str) -> tuple[Database, DaemonService]:
    config = load_config(config_path)
    db = Database(config.db_path)
    gh = GhClient(config.repo)
    poller = Poller(config=config, db=db, gh=gh)
    runner = CodexRunner(config=config, repo_root=detect_repo_root())
    pr_manager = PRManager(config=config, gh=gh)
    daemon = DaemonService(
        config=config,
        db=db,
        gh=gh,
        poller=poller,
        runner=runner,
        pr_manager=pr_manager,
    )
    return db, daemon


def cmd_status(config_path: str) -> int:
    db: Database | None = None
    try:
        db, _ = build_service(config_path)
        counts = db.get_status_counts()
        if not counts:
            print("No issues tracked yet.")
            return 0
        total = sum(counts.values())
        print(f"Total tracked issues: {total}")
        for status in sorted(counts):
            print(f"{status}: {counts[status]}")
        return 0
    finally:
        if db is not None:
            db.close()


def cmd_run_once(config_path: str) -> int:
    db: Database | None = None
    try:
        db, daemon = build_service(config_path)
        daemon.run_once()
        return 0
    finally:
        if db is not None:
            db.close()


def cmd_daemon(config_path: str) -> int:
    db: Database | None = None
    try:
        db, daemon = build_service(config_path)
        daemon.run_forever()
        return 0
    finally:
        if db is not None:
            db.close()


def cmd_doctor(config_path: str) -> int:
    config = load_config(config_path)
    results, ok = run_doctor(config=config, repo_root=detect_repo_root())
    print_doctor_report(results)
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        if args.command == "status":
            return cmd_status(args.config)
        if args.command == "run-once":
            return cmd_run_once(args.config)
        if args.command == "daemon":
            return cmd_daemon(args.config)
        if args.command == "doctor":
            return cmd_doctor(args.config)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        logging.getLogger(__name__).exception("fatal error: %s", exc)
        return 1
    parser.print_help(sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
