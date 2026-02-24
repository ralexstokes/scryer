from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import Config
from .models import RunnerResult


class RunnerError(RuntimeError):
    pass


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _short_title(title: str, max_len: int = 72) -> str:
    clean = " ".join(title.split())
    if len(clean) <= max_len:
        return clean
    return clean[: max_len - 3].rstrip() + "..."


class CodexRunner:
    _HEARTBEAT_SECONDS = 20

    def __init__(self, config: Config, repo_root: Path):
        self.config = config
        self.repo_root = repo_root
        self.log = logging.getLogger(__name__)

    def run(self, issue: dict[str, object]) -> RunnerResult:
        issue_id = int(issue["number"])
        branch = f"{self.config.branch_prefix}/issue-{issue_id}"
        worktree_path = self.config.worktrees_dir / f"issue-{issue_id}"
        run_dir = self.config.runs_dir / f"issue-{issue_id}" / f"run-{_utc_now_compact()}"
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        run_dir.mkdir(parents=True, exist_ok=True)

        prompt_text = self._build_prompt(issue)
        prompt_path = run_dir / "prompt.md"
        stdout_path = run_dir / "codex_stdout.log"
        stderr_path = run_dir / "codex_stderr.log"
        diff_path = run_dir / "git_diff.patch"
        summary_path = run_dir / "summary.json"
        prompt_path.write_text(prompt_text, encoding="utf-8")

        started_at = _utc_now_iso()
        exit_code: int | None = None
        status = "failed"
        error: str | None = None
        head_sha: str | None = None
        codex_stdout = ""
        codex_stderr = ""

        try:
            self._ensure_clean_worktree(worktree_path, branch)
            self._git(
                ["worktree", "add", "-B", branch, str(worktree_path), self.config.base_branch],
                cwd=self.repo_root,
            )
            self.log.info(
                "prepared worktree issue=%s branch=%s path=%s base=%s",
                issue_id,
                branch,
                worktree_path,
                self.config.base_branch,
            )

            cmd = self._build_codex_command()
            self.log.info(
                "starting codex issue=%s timeout_seconds=%s run_dir=%s cmd=%s",
                issue_id,
                self.config.codex_timeout_seconds,
                run_dir,
                " ".join(cmd),
            )
            proc, elapsed_seconds = self._run_codex_with_heartbeat(
                cmd,
                prompt_text=prompt_text,
                issue_id=issue_id,
                run_dir=run_dir,
                cwd=worktree_path,
                timeout_seconds=self.config.codex_timeout_seconds,
            )
            codex_stdout = proc.stdout or ""
            codex_stderr = proc.stderr or ""
            exit_code = proc.returncode
            self.log.info(
                "codex finished issue=%s exit_code=%s elapsed_seconds=%s",
                issue_id,
                exit_code,
                elapsed_seconds,
            )

            if proc.returncode != 0:
                status = "failed"
                error = f"Codex exited with code {proc.returncode}"
                self.log.error("codex failed issue=%s exit_code=%s", issue_id, proc.returncode)
            else:
                dirty = self._git_output(["status", "--porcelain"], cwd=worktree_path).strip()
                if not dirty:
                    status = "skipped"
                    error = "no changes produced"
                    self.log.info("no changes after codex issue=%s", issue_id)
                else:
                    self._git(["add", "-A"], cwd=worktree_path)
                    self._git(
                        ["commit", "-m", f"Fix #{issue_id}: {_short_title(str(issue.get('title', '')))}"],
                        cwd=worktree_path,
                    )
                    head_sha = self._git_output(["rev-parse", "HEAD"], cwd=worktree_path).strip()
                    self._git(["push", "-u", "origin", branch], cwd=worktree_path)
                    status = "pushed"
                    self.log.info("pushed branch issue=%s branch=%s head_sha=%s", issue_id, branch, head_sha)
        except subprocess.TimeoutExpired as exc:
            codex_stdout = (exc.stdout or "") if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", errors="replace")
            codex_stderr = (exc.stderr or "") if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", errors="replace")
            status = "timeout"
            error = f"Codex timed out after {self.config.codex_timeout_seconds}s"
            self.log.error(
                "codex timeout issue=%s timeout_seconds=%s",
                issue_id,
                self.config.codex_timeout_seconds,
            )
        except Exception as exc:
            status = "failed"
            error = str(exc)
            self.log.exception("runner failed issue=%s", issue_id)
        finally:
            stdout_path.write_text(codex_stdout, encoding="utf-8")
            stderr_path.write_text(codex_stderr, encoding="utf-8")
            self._write_diff(worktree_path, diff_path)

            finished_at = _utc_now_iso()
            summary = {
                "issue_id": issue_id,
                "status": status,
                "branch": branch,
                "head_sha": head_sha,
                "error": error,
                "codex_exit_code": exit_code,
                "started_at": started_at,
                "finished_at": finished_at,
                "run_dir": str(run_dir),
                "artifacts": {
                    "prompt": str(prompt_path),
                    "stdout": str(stdout_path),
                    "stderr": str(stderr_path),
                    "diff": str(diff_path),
                },
            }
            summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

            keep_worktree = self.config.keep_worktree_on_failure and status in {"failed", "timeout"}
            if not keep_worktree:
                self._cleanup_worktree(worktree_path)
            self.log.info(
                "run complete issue=%s status=%s run_dir=%s summary=%s",
                issue_id,
                status,
                run_dir,
                summary_path,
            )

        return RunnerResult(
            status=status,
            branch=branch,
            run_dir=run_dir,
            head_sha=head_sha,
            error=error,
            exit_code=exit_code,
        )

    def _ensure_clean_worktree(self, worktree_path: Path, branch: str) -> None:
        self._git_ignore_error(["worktree", "remove", "--force", str(worktree_path)], cwd=self.repo_root)
        if worktree_path.exists():
            shutil.rmtree(worktree_path, ignore_errors=True)
        self._git_ignore_error(["branch", "-D", branch], cwd=self.repo_root)

    def _cleanup_worktree(self, worktree_path: Path) -> None:
        self._git_ignore_error(["worktree", "remove", "--force", str(worktree_path)], cwd=self.repo_root)
        if worktree_path.exists():
            shutil.rmtree(worktree_path, ignore_errors=True)

    def _build_codex_command(self) -> list[str]:
        cmd = [self.config.codex_cmd]
        if self.config.codex_mode:
            cmd.append(self.config.codex_mode)
        cmd.extend(self.config.codex_args)
        if self.config.codex_model:
            cmd.extend(["--model", self.config.codex_model])
        if self.config.codex_allowed_tools:
            cmd.extend(["--allowed-tools", self.config.codex_allowed_tools])
        if self.config.codex_cost_guard:
            cmd.extend(["--cost-guard", self.config.codex_cost_guard])
        return cmd

    def _build_prompt(self, issue: dict[str, object]) -> str:
        conventions = self._load_conventions()
        issue_title = str(issue.get("title", "")).strip()
        issue_body = str(issue.get("body", "")).strip()
        issue_url = str(issue.get("url", "")).strip()
        lines = [
            "# Task",
            "Implement the enhancement described in this GitHub issue.",
            "",
            "## Issue",
            f"- Number: {issue.get('number')}",
            f"- Title: {issue_title}",
            f"- URL: {issue_url}",
            "",
            "### Body",
            issue_body if issue_body else "(No issue body provided.)",
            "",
            "## Hard Rules",
            "- Keep changes minimal and reviewable.",
            "- Do not modify unrelated files.",
            "- Run relevant tests/linters if they are available and straightforward.",
            "- If requirements are unclear, stop and explain what is missing instead of guessing.",
            "",
            "## Required Final Output",
            "- If you are ready for the final output, make a refactor pass on the full change set and include those.",
            "- A brief summary of what changed.",
            "- Exact commands used to verify the change.",
            "",
        ]
        if conventions:
            lines.append("## Repository Conventions")
            lines.extend(conventions)
            lines.append("")
        return "\n".join(lines).strip() + "\n"

    def _load_conventions(self) -> list[str]:
        sections: list[str] = []
        for filename in self.config.conventions_files:
            path = self.repo_root / filename
            if not path.exists() or not path.is_file():
                continue
            text = path.read_text(encoding="utf-8", errors="replace").strip()
            if not text:
                continue
            sections.append(f"### {filename}")
            sections.append(text)
            sections.append("")
        return sections

    def _run_codex_with_heartbeat(
        self,
        cmd: list[str],
        prompt_text: str,
        issue_id: int,
        run_dir: Path,
        cwd: Path,
        timeout_seconds: int,
    ) -> tuple[subprocess.CompletedProcess[str], int]:
        started = time.monotonic()
        input_text: str | None = prompt_text
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            while True:
                elapsed = time.monotonic() - started
                remaining = timeout_seconds - elapsed
                if remaining <= 0:
                    proc.kill()
                    stdout, stderr = proc.communicate()
                    raise subprocess.TimeoutExpired(
                        cmd=cmd,
                        timeout=timeout_seconds,
                        output=stdout,
                        stderr=stderr,
                    )

                wait_seconds = min(self._HEARTBEAT_SECONDS, max(1.0, remaining))
                try:
                    stdout, stderr = proc.communicate(input=input_text, timeout=wait_seconds)
                    done_elapsed = int(time.monotonic() - started)
                    completed = subprocess.CompletedProcess(
                        args=cmd,
                        returncode=proc.returncode,
                        stdout=stdout or "",
                        stderr=stderr or "",
                    )
                    return completed, done_elapsed
                except subprocess.TimeoutExpired:
                    input_text = None
                    self.log.info(
                        "codex still running issue=%s elapsed_seconds=%s run_dir=%s",
                        issue_id,
                        int(time.monotonic() - started),
                        run_dir,
                    )
        finally:
            if proc.stdin and not proc.stdin.closed:
                proc.stdin.close()

    def _write_diff(self, worktree_path: Path, diff_path: Path) -> None:
        if not worktree_path.exists():
            diff_path.write_text("", encoding="utf-8")
            return
        proc = subprocess.run(
            ["git", "show", "--patch", "--stat", "HEAD"],
            cwd=worktree_path,
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout:
            diff_path.write_text(proc.stdout, encoding="utf-8")
            return
        fallback = subprocess.run(
            ["git", "diff", "--patch", "--stat"],
            cwd=worktree_path,
            text=True,
            capture_output=True,
            check=False,
        )
        diff_path.write_text(fallback.stdout or "", encoding="utf-8")

    def _git(self, args: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
        cmd = ["git", *args]
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RunnerError(f"Command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr}")
        return proc

    def _git_output(self, args: list[str], cwd: Path) -> str:
        return self._git(args, cwd).stdout or ""

    def _git_ignore_error(self, args: list[str], cwd: Path) -> None:
        subprocess.run(
            ["git", *args],
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
        )
