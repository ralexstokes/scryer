from __future__ import annotations

import logging

from .config import Config
from .gh import GhClient
from .models import PrInfo, RunnerResult


class PRManager:
    def __init__(self, config: Config, gh: GhClient):
        self.config = config
        self.gh = gh
        self.log = logging.getLogger(__name__)

    def ensure_pr(self, issue: dict[str, object], result: RunnerResult) -> PrInfo:
        branch = result.branch
        existing = self.gh.list_open_pr_for_branch(branch)
        if existing:
            first = existing[0]
            self.log.info(
                "pr already open branch=%s pr=%s",
                branch,
                first.get("url"),
            )
            return PrInfo(
                number=int(first.get("number")),
                url=str(first.get("url")),
                created=False,
            )

        title = f"[Codex] {str(issue.get('title', '')).strip()}"
        body = self._build_pr_body(issue)
        self.log.info(
            "creating pr branch=%s base=%s draft=%s",
            branch,
            self.config.base_branch,
            self.config.draft_pr,
        )
        create_out = self.gh.create_pr(
            branch=branch,
            base_branch=self.config.base_branch,
            title=title,
            body=body,
            draft=self.config.draft_pr,
        )
        refreshed = self.gh.list_open_pr_for_branch(branch)
        if refreshed:
            first = refreshed[0]
            pr_number = int(first.get("number"))
            pr_url = str(first.get("url"))
        else:
            pr_number = self.gh.parse_pr_number_from_url(create_out)
            pr_url = create_out or None

        if self.config.issue_comment_on_success and pr_url:
            self.gh.comment_issue(
                int(issue["number"]),
                f"Opened PR for this issue: {pr_url}",
            )
            self.log.info("posted issue comment issue=%s pr=%s", issue["number"], pr_url)

        self.log.info("pr ready branch=%s pr_number=%s pr_url=%s", branch, pr_number, pr_url)

        return PrInfo(number=pr_number, url=pr_url, created=True)

    def _build_pr_body(self, issue: dict[str, object]) -> str:
        issue_id = int(issue["number"])
        return "\n".join(
            [
                f"Fixes #{issue_id}",
                "",
                "### What Changed",
                "- Automated implementation generated in a dedicated Codex worktree.",
                "",
                "### How To Verify",
                "- Review the PR diff and run project tests/linters.",
            ]
        )
