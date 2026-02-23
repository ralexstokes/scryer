from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .config import Config


@dataclass(slots=True)
class CheckResult:
    name: str
    ok: bool
    message: str


def _run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )


def run_doctor(config: Config, repo_root: Path) -> tuple[list[CheckResult], bool]:
    results: list[CheckResult] = []

    git_path = shutil.which("git")
    if git_path:
        results.append(CheckResult("git binary", True, git_path))
    else:
        results.append(CheckResult("git binary", False, "git not found in PATH"))

    codex_path = shutil.which(config.codex_cmd)
    if codex_path:
        results.append(CheckResult("codex binary", True, codex_path))
    else:
        results.append(
            CheckResult(
                "codex binary",
                False,
                f"{config.codex_cmd!r} not found in PATH; set codex_cmd or install Codex CLI",
            )
        )

    gh_path = shutil.which("gh")
    if gh_path:
        results.append(CheckResult("gh binary", True, gh_path))
    else:
        results.append(CheckResult("gh binary", False, "gh not found in PATH"))

    if git_path:
        proc = _run(["git", "rev-parse", "--show-toplevel"], cwd=repo_root)
        if proc.returncode == 0:
            results.append(CheckResult("git repository", True, proc.stdout.strip()))
        else:
            results.append(
                CheckResult("git repository", False, (proc.stderr or "not a git repository").strip())
            )

        remote = _run(["git", "remote", "get-url", "origin"], cwd=repo_root)
        if remote.returncode == 0 and remote.stdout.strip():
            results.append(CheckResult("git origin remote", True, remote.stdout.strip()))
        else:
            results.append(
                CheckResult(
                    "git origin remote",
                    False,
                    (remote.stderr or "missing origin remote").strip(),
                )
            )

        base_local = _run(
            ["git", "show-ref", "--verify", f"refs/heads/{config.base_branch}"],
            cwd=repo_root,
        )
        base_remote = _run(
            ["git", "show-ref", "--verify", f"refs/remotes/origin/{config.base_branch}"],
            cwd=repo_root,
        )
        if base_local.returncode == 0 or base_remote.returncode == 0:
            where = "local" if base_local.returncode == 0 else "origin"
            results.append(
                CheckResult(
                    "base branch",
                    True,
                    f"{config.base_branch} found ({where})",
                )
            )
        else:
            results.append(
                CheckResult(
                    "base branch",
                    False,
                    f"{config.base_branch} not found locally or at origin/{config.base_branch}",
                )
            )

    if gh_path:
        auth = _run(["gh", "auth", "status", "--hostname", "github.com"])
        if auth.returncode == 0:
            results.append(CheckResult("gh auth", True, "authenticated"))
        else:
            msg = auth.stderr.strip() or auth.stdout.strip() or "authentication check failed"
            results.append(CheckResult("gh auth", False, msg))

        view = _run(
            ["gh", "repo", "view", "--json", "nameWithOwner,defaultBranchRef"],
            cwd=repo_root,
        )
        if view.returncode == 0:
            repo_name = "inferred repository"
            try:
                payload = json.loads(view.stdout) if view.stdout.strip() else {}
                if isinstance(payload, dict):
                    resolved = payload.get("nameWithOwner")
                    if isinstance(resolved, str) and resolved.strip():
                        repo_name = resolved.strip()
            except json.JSONDecodeError:
                pass
            results.append(CheckResult("repo access", True, repo_name))
        else:
            msg = view.stderr.strip() or view.stdout.strip() or "cannot access inferred repository"
            results.append(CheckResult("repo access", False, msg))

    try:
        config.workdir.mkdir(parents=True, exist_ok=True)
        probe = config.workdir / ".doctor_write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        results.append(CheckResult("workdir writable", True, str(config.workdir)))
    except Exception as exc:
        results.append(CheckResult("workdir writable", False, str(exc)))

    try:
        config.db_path.parent.mkdir(parents=True, exist_ok=True)
        results.append(CheckResult("db path parent", True, str(config.db_path.parent)))
    except Exception as exc:
        results.append(CheckResult("db path parent", False, str(exc)))

    success = all(item.ok for item in results)
    return results, success


def print_doctor_report(results: list[CheckResult]) -> None:
    for item in results:
        status = "PASS" if item.ok else "FAIL"
        print(f"[{status}] {item.name}: {item.message}")
