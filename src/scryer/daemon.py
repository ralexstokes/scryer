from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass
from datetime import date

from .config import Config
from .db import Database
from .gh import GhClient, GhError
from .models import IssueRecord
from .poller import Poller
from .pr import PRManager
from .runner import CodexRunner


@dataclass(slots=True)
class CycleResult:
    processed: bool
    status: str | None = None


class DaemonService:
    def __init__(
        self,
        config: Config,
        db: Database,
        gh: GhClient,
        poller: Poller,
        runner: CodexRunner,
        pr_manager: PRManager,
    ):
        self.config = config
        self.db = db
        self.gh = gh
        self.poller = poller
        self.runner = runner
        self.pr_manager = pr_manager
        self.log = logging.getLogger(__name__)
        self._stop_requested = False

    def install_signal_handlers(self) -> None:
        def _handler(signum: int, _frame) -> None:
            self.log.info("signal received signum=%s stop_requested=true", signum)
            self._stop_requested = True

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def run_forever(self) -> None:
        self.install_signal_handlers()
        gh_backoff = self.config.poll_interval_seconds
        consecutive_failures = 0

        while not self._stop_requested:
            try:
                result = self.run_once()
                gh_backoff = self.config.poll_interval_seconds
            except GhError as exc:
                wait_seconds = min(gh_backoff, 300)
                self.log.error("github operation failed backoff_seconds=%s error=%s", wait_seconds, exc)
                gh_backoff = min(gh_backoff * 2, 300)
                self._sleep_interruptible(wait_seconds)
                continue
            except Exception:
                self.log.exception("unexpected daemon loop error")
                self._sleep_interruptible(self.config.poll_interval_seconds)
                continue

            if result.status in {"failed", "timeout"}:
                consecutive_failures += 1
            elif result.processed:
                consecutive_failures = 0

            if consecutive_failures >= 3:
                extra_delay = min(self.config.poll_interval_seconds * 3, 300)
                self.log.warning(
                    "consecutive failures threshold reached count=%s wait_seconds=%s",
                    consecutive_failures,
                    extra_delay,
                )
                self._sleep_interruptible(extra_delay)
            else:
                self._sleep_interruptible(self.config.poll_interval_seconds)

    def run_once(self) -> CycleResult:
        self.poller.poll_and_upsert()
        expired = self.db.requeue_expired_leases()
        if expired:
            self.log.info("requeued expired leases count=%s", expired)

        if self._daily_limit_reached():
            self.log.warning("daily issue limit reached limit=%s", self.config.max_issues_per_day)
            return CycleResult(processed=False, status=None)

        issue = self.db.claim_next_pending(
            worker_id=self.config.worker_id,
            max_attempts=self.config.max_attempts,
            lease_seconds=self.config.lease_seconds,
        )
        if issue is None:
            self.log.info("no pending issues available")
            return CycleResult(processed=False, status=None)

        return self._handle_issue(issue)

    def _handle_issue(self, issue: IssueRecord) -> CycleResult:
        self.log.info("claimed issue id=%s attempt=%s", issue.id, issue.attempt_count)
        run_dir: str | None = None
        try:
            full = self.gh.view_issue(issue.id)
            label_names = self._label_names(full)
            self.db.update_issue_details(
                {
                    "id": int(full["number"]),
                    "title": str(full.get("title", "")),
                    "body": full.get("body"),
                    "url": full.get("url"),
                    "labels": label_names,
                    "updated_at": full.get("updatedAt"),
                }
            )

            if str(full.get("state", "")).lower() != "open":
                reason = "issue is no longer open"
                self.db.mark_skipped(issue.id, reason, run_dir)
                return CycleResult(processed=True, status="skipped")

            if self.config.trigger_label not in label_names:
                reason = f"missing trigger label '{self.config.trigger_label}'"
                self.db.mark_skipped(issue.id, reason, run_dir)
                return CycleResult(processed=True, status="skipped")

            skip_hit = sorted({label for label in label_names if label in set(self.config.skip_labels)})
            if skip_hit:
                reason = f"contains skip label(s): {', '.join(skip_hit)}"
                self.db.mark_skipped(issue.id, reason, run_dir)
                return CycleResult(processed=True, status="skipped")

            result = self.runner.run(full)
            run_dir = str(result.run_dir)
            if result.status == "pushed":
                pr = self.pr_manager.ensure_pr(full, result)
                self.db.mark_done(
                    issue_id=issue.id,
                    pr_number=pr.number,
                    pr_url=pr.url,
                    branch=result.branch,
                    head_sha=result.head_sha,
                    run_dir=run_dir,
                )
                self._increment_daily_count()
                self.log.info("issue complete id=%s pr=%s", issue.id, pr.url)
                return CycleResult(processed=True, status="done")

            if result.status == "skipped":
                self.db.mark_skipped(issue.id, result.error or "no changes produced", run_dir)
                self.log.info("issue skipped id=%s reason=%s", issue.id, result.error)
                return CycleResult(processed=True, status="skipped")

            if result.status == "timeout":
                self.db.mark_timeout(issue.id, result.error or "runner timeout", run_dir)
                self.log.warning("issue timed out id=%s", issue.id)
                return CycleResult(processed=True, status="timeout")

            self.db.mark_failed(issue.id, result.error or "runner failed", run_dir)
            self.log.error("issue failed id=%s error=%s", issue.id, result.error)
            return CycleResult(processed=True, status="failed")
        except Exception as exc:
            self.db.mark_failed(issue.id, str(exc), run_dir)
            self.log.exception("issue handling exception id=%s", issue.id)
            return CycleResult(processed=True, status="failed")

    def _daily_limit_reached(self) -> bool:
        today = date.today().isoformat()
        done_count = self.db.get_daily_done_count(today)
        return done_count >= self.config.max_issues_per_day

    def _increment_daily_count(self) -> None:
        today = date.today().isoformat()
        self.db.increment_daily_done_count(today)

    @staticmethod
    def _label_names(issue: dict[str, object]) -> list[str]:
        labels = []
        raw = issue.get("labels", [])
        if not isinstance(raw, list):
            return labels
        for label in raw:
            if isinstance(label, dict) and label.get("name"):
                labels.append(str(label["name"]))
        return labels

    def _sleep_interruptible(self, seconds: int) -> None:
        deadline = time.monotonic() + max(0, seconds)
        while not self._stop_requested and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
