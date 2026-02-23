from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
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
        cycle = 0
        self.log.info(
            "daemon started worker=%s poll_interval_seconds=%s lease_seconds=%s max_attempts=%s max_concurrent=%s",
            self.config.worker_id,
            self.config.poll_interval_seconds,
            self.config.lease_seconds,
            self.config.max_attempts,
            self.config.max_concurrent,
        )

        while not self._stop_requested:
            cycle += 1
            cycle_started = time.monotonic()
            sleep_seconds = self.config.poll_interval_seconds
            try:
                result = self.run_once()
                gh_backoff = self.config.poll_interval_seconds
            except GhError as exc:
                wait_seconds = min(gh_backoff, 300)
                self.log.error(
                    "github operation failed cycle=%s backoff_seconds=%s error=%s",
                    cycle,
                    wait_seconds,
                    exc,
                )
                gh_backoff = min(gh_backoff * 2, 300)
                sleep_seconds = wait_seconds
                self.log.info("cycle sleep cycle=%s sleep_seconds=%s", cycle, sleep_seconds)
                self._sleep_interruptible(sleep_seconds)
                continue
            except Exception:
                self.log.exception("unexpected daemon loop error cycle=%s", cycle)
                sleep_seconds = self.config.poll_interval_seconds
                self.log.info("cycle sleep cycle=%s sleep_seconds=%s", cycle, sleep_seconds)
                self._sleep_interruptible(sleep_seconds)
                continue

            elapsed_seconds = int(time.monotonic() - cycle_started)
            self.log.info(
                "cycle complete cycle=%s processed=%s status=%s elapsed_seconds=%s",
                cycle,
                result.processed,
                result.status,
                elapsed_seconds,
            )

            if result.status in {"failed", "timeout"}:
                consecutive_failures += 1
            elif result.processed:
                consecutive_failures = 0

            if consecutive_failures >= 3:
                extra_delay = min(self.config.poll_interval_seconds * 3, 300)
                self.log.warning(
                    "consecutive failures threshold reached cycle=%s count=%s wait_seconds=%s",
                    cycle,
                    consecutive_failures,
                    extra_delay,
                )
                sleep_seconds = extra_delay
            else:
                sleep_seconds = self.config.poll_interval_seconds
            self.log.info("cycle sleep cycle=%s sleep_seconds=%s", cycle, sleep_seconds)
            self._sleep_interruptible(sleep_seconds)
        self.log.info("daemon stopped")

    def run_once(self, issue_id: int | None = None) -> CycleResult:
        polled = self.poller.poll_and_upsert()
        self.log.info("poll sync complete fetched=%s", polled)
        expired = self.db.requeue_expired_leases()
        if expired:
            self.log.info("requeued expired leases count=%s", expired)

        if issue_id is not None:
            issue = self._claim_target_issue(issue_id)
            if issue is None:
                self.log.info("requested issue is not pending id=%s", issue_id)
                return CycleResult(processed=False, status=None)
            return self._handle_issue(issue, self.db)

        claim_limit = self._claim_limit_for_cycle()
        if claim_limit <= 0:
            self.log.warning("daily issue limit reached limit=%s", self.config.max_issues_per_day)
            return CycleResult(processed=False, status=None)

        issues = self._claim_pending_batch(claim_limit)
        if not issues:
            self.log.info("no pending issues available")
            return CycleResult(processed=False, status=None)
        self.log.info(
            "claimed issues count=%s max_concurrent=%s issue_ids=%s",
            len(issues),
            self.config.max_concurrent,
            ",".join(str(issue.id) for issue in issues),
        )
        return self._process_claimed_issues(issues)

    def _claim_limit_for_cycle(self) -> int:
        daily_remaining = self._daily_remaining_capacity()
        if daily_remaining <= 0:
            return 0
        return min(max(1, self.config.max_concurrent), daily_remaining)

    def _daily_remaining_capacity(self) -> int:
        today = date.today().isoformat()
        done_count = self.db.get_daily_done_count(today)
        return max(self.config.max_issues_per_day - done_count, 0)

    def _claim_pending_batch(self, claim_limit: int) -> list[IssueRecord]:
        claimed: list[IssueRecord] = []
        for _ in range(claim_limit):
            issue = self.db.claim_next_pending(
                worker_id=self.config.worker_id,
                max_attempts=self.config.max_attempts,
                lease_seconds=self.config.lease_seconds,
            )
            if issue is None:
                break
            claimed.append(issue)
        return claimed

    def _process_claimed_issues(self, issues: list[IssueRecord]) -> CycleResult:
        if len(issues) == 1:
            return self._handle_issue(issues[0], self.db)

        results: list[CycleResult] = []
        with ThreadPoolExecutor(max_workers=len(issues), thread_name_prefix="scryer-worker") as executor:
            futures = [executor.submit(self._handle_issue_with_worker_db, issue) for issue in issues]
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception:
                    self.log.exception("parallel worker failed unexpectedly")
                    results.append(CycleResult(processed=True, status="failed"))

        statuses = [result.status for result in results if result.status]
        return CycleResult(
            processed=any(result.processed for result in results),
            status=self._aggregate_status(statuses),
        )

    def _handle_issue_with_worker_db(self, issue: IssueRecord) -> CycleResult:
        worker_db = Database(self.config.db_path)
        try:
            return self._handle_issue(issue, worker_db)
        finally:
            worker_db.close()

    @staticmethod
    def _aggregate_status(statuses: list[str]) -> str | None:
        if not statuses:
            return None
        if all(status in {"failed", "timeout"} for status in statuses):
            if "failed" in statuses:
                return "failed"
            return "timeout"
        if "done" in statuses:
            return "done"
        if "skipped" in statuses:
            return "skipped"
        if "timeout" in statuses:
            return "timeout"
        if "failed" in statuses:
            return "failed"
        return statuses[0]

    def _claim_target_issue(self, issue_id: int) -> IssueRecord | None:
        issue = self.db.claim_pending_by_id(
            issue_id=issue_id,
            worker_id=self.config.worker_id,
            max_attempts=self.config.max_attempts,
            lease_seconds=self.config.lease_seconds,
        )
        if issue is not None:
            return issue

        full = self.gh.view_issue(issue_id)
        labels = self._label_names(full)
        self.db.upsert_polled_issues(
            [
                {
                    "id": int(full["number"]),
                    "title": str(full.get("title", "")),
                    "body": full.get("body"),
                    "url": full.get("url"),
                    "labels": labels,
                    "updated_at": full.get("updatedAt"),
                }
            ]
        )
        return self.db.claim_pending_by_id(
            issue_id=issue_id,
            worker_id=self.config.worker_id,
            max_attempts=self.config.max_attempts,
            lease_seconds=self.config.lease_seconds,
        )

    def _handle_issue(self, issue: IssueRecord, db: Database) -> CycleResult:
        self.log.info("claimed issue id=%s attempt=%s", issue.id, issue.attempt_count)
        run_dir: str | None = None
        try:
            full = self.gh.view_issue(issue.id)
            label_names = self._label_names(full)
            db.update_issue_details(
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
                db.mark_skipped(issue.id, reason, run_dir)
                self.log.info("issue skipped id=%s reason=%s", issue.id, reason)
                return CycleResult(processed=True, status="skipped")

            if self.config.trigger_label not in label_names:
                reason = f"missing trigger label '{self.config.trigger_label}'"
                db.mark_skipped(issue.id, reason, run_dir)
                self.log.info("issue skipped id=%s reason=%s", issue.id, reason)
                return CycleResult(processed=True, status="skipped")

            skip_hit = sorted({label for label in label_names if label in set(self.config.skip_labels)})
            if skip_hit:
                reason = f"contains skip label(s): {', '.join(skip_hit)}"
                db.mark_skipped(issue.id, reason, run_dir)
                self.log.info("issue skipped id=%s reason=%s", issue.id, reason)
                return CycleResult(processed=True, status="skipped")

            result = self.runner.run(full)
            run_dir = str(result.run_dir)
            self.log.info(
                "runner result id=%s status=%s branch=%s run_dir=%s",
                issue.id,
                result.status,
                result.branch,
                run_dir,
            )
            if result.status == "pushed":
                pr = self.pr_manager.ensure_pr(full, result)
                db.mark_done(
                    issue_id=issue.id,
                    pr_number=pr.number,
                    pr_url=pr.url,
                    branch=result.branch,
                    head_sha=result.head_sha,
                    run_dir=run_dir,
                )
                self._increment_daily_count(db)
                self.log.info("issue complete id=%s pr=%s", issue.id, pr.url)
                return CycleResult(processed=True, status="done")

            if result.status == "skipped":
                db.mark_skipped(issue.id, result.error or "no changes produced", run_dir)
                self.log.info("issue skipped id=%s reason=%s", issue.id, result.error)
                return CycleResult(processed=True, status="skipped")

            if result.status == "timeout":
                db.mark_timeout(issue.id, result.error or "runner timeout", run_dir)
                self.log.warning("issue timed out id=%s", issue.id)
                return CycleResult(processed=True, status="timeout")

            db.mark_failed(issue.id, result.error or "runner failed", run_dir)
            self.log.error("issue failed id=%s error=%s", issue.id, result.error)
            return CycleResult(processed=True, status="failed")
        except Exception as exc:
            db.mark_failed(issue.id, str(exc), run_dir)
            self.log.exception("issue handling exception id=%s", issue.id)
            return CycleResult(processed=True, status="failed")

    def _increment_daily_count(self, db: Database) -> None:
        today = date.today().isoformat()
        db.increment_daily_done_count(today)

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
