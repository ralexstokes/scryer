from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class GhError(RuntimeError):
    cmd: list[str]
    exit_code: int
    stdout: str
    stderr: str

    def __str__(self) -> str:
        return (
            f"GitHub CLI command failed ({self.exit_code}): {' '.join(self.cmd)}\n"
            f"stderr: {self.stderr.strip()}"
        )


class GhClient:
    def __init__(self, repo: str):
        self.repo = repo

    def _run(self, args: list[str]) -> str:
        cmd = ["gh", *args]
        proc = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            raise GhError(
                cmd=cmd,
                exit_code=proc.returncode,
                stdout=proc.stdout or "",
                stderr=proc.stderr or "",
            )
        return proc.stdout or ""

    def gh_json(self, args: list[str]) -> Any:
        raw = self._run(args)
        if not raw.strip():
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise GhError(args, 1, raw, f"Invalid JSON from gh: {exc}") from exc

    def gh_text(self, args: list[str]) -> str:
        return self._run(args)

    def list_open_issues(self, trigger_label: str, limit: int = 100) -> list[dict[str, Any]]:
        query = f"is:issue is:open label:{trigger_label} sort:updated-desc"
        data = self.gh_json(
            [
                "issue",
                "list",
                "--repo",
                self.repo,
                "--search",
                query,
                "--limit",
                str(limit),
                "--json",
                "number,title,updatedAt,createdAt,url,labels",
            ]
        )
        if not isinstance(data, list):
            return []
        return data

    def view_issue(self, issue_id: int) -> dict[str, Any]:
        data = self.gh_json(
            [
                "issue",
                "view",
                str(issue_id),
                "--repo",
                self.repo,
                "--json",
                "number,title,body,url,labels,updatedAt,state",
            ]
        )
        if not isinstance(data, dict):
            raise GhError(["issue", "view", str(issue_id)], 1, str(data), "Unexpected issue payload")
        return data

    def list_open_pr_for_branch(self, branch: str) -> list[dict[str, Any]]:
        data = self.gh_json(
            [
                "pr",
                "list",
                "--repo",
                self.repo,
                "--head",
                branch,
                "--state",
                "open",
                "--json",
                "number,url",
            ]
        )
        if not isinstance(data, list):
            return []
        return data

    def create_pr(
        self,
        branch: str,
        base_branch: str,
        title: str,
        body: str,
        draft: bool,
    ) -> str:
        args = [
            "pr",
            "create",
            "--repo",
            self.repo,
            "--head",
            branch,
            "--base",
            base_branch,
            "--title",
            title,
            "--body",
            body,
        ]
        if draft:
            args.append("--draft")
        out = self.gh_text(args).strip()
        return out

    def comment_issue(self, issue_id: int, body: str) -> None:
        self.gh_text(
            [
                "issue",
                "comment",
                str(issue_id),
                "--repo",
                self.repo,
                "--body",
                body,
            ]
        )

    @staticmethod
    def parse_pr_number_from_url(url: str | None) -> int | None:
        if not url:
            return None
        match = re.search(r"/pull/(\d+)", url)
        if not match:
            return None
        return int(match.group(1))

