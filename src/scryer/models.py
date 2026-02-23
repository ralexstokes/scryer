from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class IssueRecord:
    id: int
    title: str
    body: str | None
    url: str | None
    labels: list[str]
    status: str
    attempt_count: int
    updated_at: str | None
    lease_until: str | None
    claimed_by: str | None
    started_at: str | None
    completed_at: str | None
    last_error: str | None
    last_run_dir: str | None


@dataclass(slots=True)
class RunnerResult:
    status: str  # pushed|skipped|failed|timeout
    branch: str
    run_dir: Path
    head_sha: str | None
    error: str | None
    exit_code: int | None


@dataclass(slots=True)
class PrInfo:
    number: int | None
    url: str | None
    created: bool

