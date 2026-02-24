from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from pathlib import Path
import tomllib


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _coalesce_env(name: str) -> str | None:
    prefixed = f"SCRYER_{name}"
    if prefixed in os.environ:
        return os.environ[prefixed]
    if name in os.environ:
        return os.environ[name]
    return None


def default_config_path() -> Path:
    xdg_config_home = os.environ.get("XDG_CONFIG_HOME")
    if xdg_config_home:
        base = Path(xdg_config_home).expanduser()
    else:
        base = Path.home() / ".config"
    if not base.is_absolute():
        base = (Path.cwd() / base).resolve()
    return (base / "scryer" / "config.toml").resolve()


@dataclass(slots=True)
class Config:
    workdir: Path
    db_path: Path
    repo_namespace: str = "default"
    trigger_label: str = "enhancement"
    base_branch: str = "main"
    poll_interval_seconds: int = 60
    codex_timeout_seconds: int = 900
    max_concurrent: int = 1
    lease_seconds: int = 2400
    max_attempts: int = 2
    branch_prefix: str = "codex"
    codex_cmd: str = "codex"
    codex_args: list[str] = field(default_factory=list)
    codex_mode: str = "run"
    codex_allowed_tools: str | None = None
    codex_model: str | None = None
    codex_cost_guard: str | None = None
    max_issues_per_day: int = 10
    skip_labels: list[str] = field(default_factory=lambda: ["wontfix", "blocked"])
    conventions_files: list[str] = field(
        default_factory=lambda: ["AGENTS.md", "CONTRIBUTING.md", "README.md"]
    )
    keep_worktree_on_failure: bool = False
    draft_pr: bool = True
    issue_comment_on_success: bool = False
    worker_id: str = field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")

    def ensure_directories(self) -> None:
        self.workdir.mkdir(parents=True, exist_ok=True)
        (self.workdir / "runs").mkdir(parents=True, exist_ok=True)
        (self.workdir / "worktrees").mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def ensure_repo_directories(self) -> None:
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.worktrees_dir.mkdir(parents=True, exist_ok=True)

    @property
    def runs_dir(self) -> Path:
        return self.workdir / "runs" / self.repo_namespace

    @property
    def worktrees_dir(self) -> Path:
        return self.workdir / "worktrees" / self.repo_namespace


def load_config(config_path: str | Path | None = None) -> Config:
    raw: dict[str, object] = {}
    resolved_config_path: Path | None = None
    default_path = default_config_path()
    if config_path:
        candidate = Path(config_path).expanduser()
        if candidate.exists():
            resolved_config_path = candidate.resolve()
            with candidate.open("rb") as handle:
                raw = tomllib.load(handle)
        elif candidate.resolve() != default_path:
            raise FileNotFoundError(f"Config file not found: {config_path}")
    elif default_path.exists():
        resolved_config_path = default_path
        with default_path.open("rb") as handle:
            raw = tomllib.load(handle)

    config_dir = resolved_config_path.parent if resolved_config_path else Path.cwd()

    workdir_raw = str(raw.get("workdir") or _coalesce_env("WORKDIR") or "./.scryer")
    workdir = Path(workdir_raw).expanduser()
    if not workdir.is_absolute():
        workdir = (config_dir / workdir).resolve()

    db_path_raw = str(
        raw.get("db_path")
        or _coalesce_env("DB_PATH")
        or str((Path(workdir_raw) / "state.db").as_posix())
    )
    db_path = Path(db_path_raw).expanduser()
    if not db_path.is_absolute():
        db_path = (config_dir / db_path).resolve()

    def int_value(key: str, default: int) -> int:
        env = _coalesce_env(key.upper())
        val = raw.get(key)
        if env is not None:
            return int(env)
        if val is None:
            return default
        return int(val)

    def str_value(key: str, default: str) -> str:
        env = _coalesce_env(key.upper())
        if env is not None:
            return env
        val = raw.get(key)
        return default if val is None else str(val)

    def optional_str_value(key: str) -> str | None:
        env = _coalesce_env(key.upper())
        if env is not None:
            return env
        val = raw.get(key)
        if val is None:
            return None
        val_str = str(val).strip()
        return val_str or None

    def list_value(key: str, default: list[str]) -> list[str]:
        env = _coalesce_env(key.upper())
        if env is not None:
            return _parse_list(env)
        val = raw.get(key)
        if val is None:
            return list(default)
        if isinstance(val, list):
            return [str(item) for item in val]
        return _parse_list(str(val))

    def bool_value(key: str, default: bool) -> bool:
        env = _coalesce_env(key.upper())
        if env is not None:
            return _parse_bool(env)
        val = raw.get(key)
        if val is None:
            return default
        if isinstance(val, bool):
            return val
        return _parse_bool(str(val))

    cfg = Config(
        workdir=workdir,
        db_path=db_path,
        trigger_label=str_value("trigger_label", "enhancement"),
        base_branch=str_value("base_branch", "main"),
        poll_interval_seconds=int_value("poll_interval_seconds", 60),
        codex_timeout_seconds=int_value("codex_timeout_seconds", 900),
        max_concurrent=int_value("max_concurrent", 1),
        lease_seconds=int_value("lease_seconds", 2400),
        max_attempts=int_value("max_attempts", 2),
        branch_prefix=str_value("branch_prefix", "codex"),
        codex_cmd=str_value("codex_cmd", "codex"),
        codex_args=list_value("codex_args", []),
        codex_mode=str_value("codex_mode", "run"),
        codex_allowed_tools=optional_str_value("codex_allowed_tools"),
        codex_model=optional_str_value("codex_model"),
        codex_cost_guard=optional_str_value("codex_cost_guard"),
        max_issues_per_day=int_value("max_issues_per_day", 10),
        skip_labels=list_value("skip_labels", ["wontfix", "blocked"]),
        conventions_files=list_value(
            "conventions_files", ["AGENTS.md", "CONTRIBUTING.md", "README.md"]
        ),
        keep_worktree_on_failure=bool_value("keep_worktree_on_failure", False),
        draft_pr=bool_value("draft_pr", True),
        issue_comment_on_success=bool_value("issue_comment_on_success", False),
    )
    cfg.ensure_directories()
    return cfg
