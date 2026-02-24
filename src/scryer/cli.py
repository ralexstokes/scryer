from __future__ import annotations

import argparse
import hashlib
import logging
import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

from .config import Config, default_config_path, load_config
from .daemon import DaemonService
from .db import Database
from .doctor import print_doctor_report, run_doctor
from .gh import GhClient
from .poller import Poller
from .pr import PRManager
from .runner import CodexRunner


def detect_repo_root(repo_root: str | None = None) -> Path:
    if repo_root:
        candidate = Path(repo_root).expanduser().resolve()
        if not candidate.exists():
            raise FileNotFoundError(f"Repo root not found: {candidate}")
        if not candidate.is_dir():
            raise NotADirectoryError(f"Repo root is not a directory: {candidate}")
        proc = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=candidate,
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode == 0:
            return Path(proc.stdout.strip()).resolve()
        return candidate

    proc = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return Path.cwd()
    return Path(proc.stdout.strip()).resolve()


def derive_repo_namespace(repo_root: Path) -> str:
    remote_namespace = _namespace_from_origin(repo_root)
    if remote_namespace:
        return remote_namespace

    resolved = repo_root.resolve()
    repo_name = re.sub(r"[^A-Za-z0-9._-]+", "-", resolved.name.lower()).strip("-._")
    if not repo_name:
        repo_name = "repo"
    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:12]
    return f"{repo_name}-{digest}"


def _namespace_from_origin(repo_root: Path) -> str | None:
    proc = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return None
    slug = _parse_remote_slug(proc.stdout.strip())
    if slug is None:
        return None
    host, owner, repo = slug
    host_ns = re.sub(r"[^A-Za-z0-9._-]+", "-", host.lower()).strip("-._") or "host"
    owner_ns = re.sub(r"[^A-Za-z0-9._-]+", "-", owner.lower()).strip("-._") or "owner"
    repo_ns = re.sub(r"[^A-Za-z0-9._-]+", "-", repo.lower()).strip("-._") or "repo"
    return f"{host_ns}-{owner_ns}-{repo_ns}"


def _parse_remote_slug(remote_url: str) -> tuple[str, str, str] | None:
    if not remote_url:
        return None

    host = ""
    path = ""

    if "://" in remote_url:
        parsed = urlsplit(remote_url)
        host = parsed.hostname or ""
        path = parsed.path
    else:
        match = re.match(r"^(?:[^@]+@)?([^:]+):(.+)$", remote_url)
        if not match:
            return None
        host = match.group(1)
        path = "/" + match.group(2)

    parts = [part for part in path.split("/") if part]
    if len(parts) < 2:
        return None
    owner = parts[-2]
    repo = parts[-1]
    if repo.endswith(".git"):
        repo = repo[:-4]
    if not host or not owner or not repo:
        return None
    return host, owner, repo


def load_scoped_config(config_path: str, repo_root: Path) -> Config:
    config = load_config(config_path)
    config.repo_namespace = derive_repo_namespace(repo_root)
    return config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scryer GitHub enhancement issue daemon")

    def add_common_args(target: argparse.ArgumentParser, *, with_defaults: bool) -> None:
        config_default = str(default_config_path()) if with_defaults else argparse.SUPPRESS
        log_default = "INFO" if with_defaults else argparse.SUPPRESS
        log_file_default = None if with_defaults else argparse.SUPPRESS
        repo_root_default = None if with_defaults else argparse.SUPPRESS
        target.add_argument("--config", default=config_default, help="Path to config TOML file")
        target.add_argument(
            "--repo-root",
            default=repo_root_default,
            help="Path to git repository root to operate on (defaults to current repository)",
        )
        target.add_argument(
            "--log-level",
            default=log_default,
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            help="Logging level",
        )
        target.add_argument(
            "--log-file",
            default=log_file_default,
            help="Optional path to a log file (logs are still written to stderr)",
        )

    add_common_args(parser, with_defaults=True)

    sub = parser.add_subparsers(dest="command", required=True)
    add_common_args(
        sub.add_parser("status", help="Show issue status counts for the active repository"),
        with_defaults=False,
    )
    run_once_parser = sub.add_parser("run-once", help="Run one poll/claim/execute cycle")
    add_common_args(
        run_once_parser,
        with_defaults=False,
    )
    run_once_parser.add_argument(
        "--issue",
        "--issue-id",
        dest="issue_id",
        type=int,
        help="Process this GitHub issue number instead of the next pending issue",
    )
    add_common_args(
        sub.add_parser("daemon", help="Run the continuous daemon loop"),
        with_defaults=False,
    )
    add_common_args(
        sub.add_parser("doctor", help="Run environment and integration readiness checks"),
        with_defaults=False,
    )
    add_common_args(
        sub.add_parser(
            "clean",
            help="Reset local state for the active repository namespace",
        ),
        with_defaults=False,
    )
    return parser


def build_service(config_path: str, repo_root: Path) -> tuple[Database, DaemonService]:
    config = load_scoped_config(config_path, repo_root)
    config.ensure_repo_directories()
    db = Database(config.db_path, repo_namespace=config.repo_namespace)
    gh = GhClient(repo_root)
    poller = Poller(config=config, db=db, gh=gh)
    runner = CodexRunner(config=config, repo_root=repo_root)
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


def cmd_status(config_path: str, repo_root: Path) -> int:
    db: Database | None = None
    try:
        db, _ = build_service(config_path, repo_root)
        counts = db.get_status_counts()
        if not counts:
            print(f"No issues tracked yet for repo namespace: {db.repo_namespace}")
            return 0
        total = sum(counts.values())
        print(f"Repo namespace: {db.repo_namespace}")
        print(f"Total tracked issues: {total}")
        for status in sorted(counts):
            print(f"{status}: {counts[status]}")
        return 0
    finally:
        if db is not None:
            db.close()


def cmd_run_once(config_path: str, repo_root: Path, issue_id: int | None = None) -> int:
    db: Database | None = None
    try:
        db, daemon = build_service(config_path, repo_root)
        daemon.run_once(issue_id=issue_id)
        return 0
    finally:
        if db is not None:
            db.close()


def cmd_daemon(config_path: str, repo_root: Path) -> int:
    db: Database | None = None
    try:
        db, daemon = build_service(config_path, repo_root)
        daemon.run_forever()
        return 0
    finally:
        if db is not None:
            db.close()


def cmd_doctor(config_path: str, repo_root: Path) -> int:
    config = load_scoped_config(config_path, repo_root)
    results, ok = run_doctor(config=config, repo_root=repo_root)
    print_doctor_report(results)
    return 0 if ok else 1


def _path_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _list_git_worktrees(repo_root: Path) -> list[Path]:
    proc = subprocess.run(
        ["git", "worktree", "list", "--porcelain"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git worktree list failed")
    worktrees: list[Path] = []
    for line in proc.stdout.splitlines():
        if line.startswith("worktree "):
            raw_path = line.split(" ", 1)[1].strip()
            if raw_path:
                worktrees.append(Path(raw_path).expanduser().resolve())
    return worktrees


def _git_worktree_remove(repo_root: Path, worktree_path: Path) -> None:
    proc = subprocess.run(
        ["git", "worktree", "remove", "--force", str(worktree_path)],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"failed to remove worktree: {worktree_path}")


def _git_worktree_prune(repo_root: Path) -> None:
    proc = subprocess.run(
        ["git", "worktree", "prune"],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git worktree prune failed")


def _remove_path(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
        return
    path.unlink()


def cmd_clean(config_path: str, repo_root: Path) -> int:
    config = load_scoped_config(config_path, repo_root)
    config.ensure_repo_directories()
    managed_worktrees = config.worktrees_dir.resolve()
    managed_runs = config.runs_dir.resolve()
    db_path = config.db_path.resolve()

    removed_worktrees = 0
    for path in _list_git_worktrees(repo_root):
        if path == repo_root:
            continue
        if not _path_within(path, managed_worktrees):
            continue
        _git_worktree_remove(repo_root, path)
        removed_worktrees += 1
    _git_worktree_prune(repo_root)

    _remove_path(managed_worktrees)
    managed_worktrees.mkdir(parents=True, exist_ok=True)

    _remove_path(managed_runs)
    managed_runs.mkdir(parents=True, exist_ok=True)

    if db_path.exists() and db_path.is_dir():
        raise RuntimeError(f"Refusing to use directory db_path: {db_path}")
    db = Database(db_path, repo_namespace=config.repo_namespace)
    cleared_issues, cleared_meta = db.clear_namespace_state()
    db.close()

    print("Reset complete:")
    print(f"- repo namespace: {config.repo_namespace}")
    print(f"- removed git worktrees: {removed_worktrees}")
    print(f"- reset worktrees dir: {managed_worktrees}")
    print(f"- reset runs dir: {managed_runs}")
    print(f"- cleared db rows: issues={cleared_issues} meta={cleared_meta}")
    print(f"- db file: {db_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo_root = detect_repo_root(getattr(args, "repo_root", None))
    log_file = getattr(args, "log_file", None)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        resolved_log_file = Path(log_file).expanduser().resolve()
        resolved_log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(resolved_log_file, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=handlers,
    )
    if log_file:
        logging.getLogger(__name__).info("file logging enabled path=%s", resolved_log_file)

    try:
        if args.command == "status":
            return cmd_status(args.config, repo_root)
        if args.command == "run-once":
            return cmd_run_once(args.config, repo_root, getattr(args, "issue_id", None))
        if args.command == "daemon":
            return cmd_daemon(args.config, repo_root)
        if args.command == "doctor":
            return cmd_doctor(args.config, repo_root)
        if args.command == "clean":
            return cmd_clean(args.config, repo_root)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        logging.getLogger(__name__).exception("fatal error: %s", exc)
        return 1
    parser.print_help(sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
