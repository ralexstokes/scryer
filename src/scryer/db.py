from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .models import IssueRecord

_SCHEMA_VERSION = 2


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_labels(labels_json: str | None) -> list[str]:
    if not labels_json:
        return []
    try:
        parsed = json.loads(labels_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(label) for label in parsed]


def _row_to_issue(row: sqlite3.Row) -> IssueRecord:
    return IssueRecord(
        id=int(row["id"]),
        title=str(row["title"]),
        body=row["body"],
        url=row["url"],
        labels=_parse_labels(row["labels_json"]),
        status=str(row["status"]),
        attempt_count=int(row["attempt_count"]),
        updated_at=row["updated_at"],
        lease_until=row["lease_until"],
        claimed_by=row["claimed_by"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        last_error=row["last_error"],
        last_run_dir=row["last_run_dir"],
    )


class Database:
    def __init__(self, db_path: str | Path, repo_namespace: str = "default"):
        self.db_path = Path(db_path)
        self.repo_namespace = repo_namespace
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._migrate()

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()

    @contextmanager
    def _begin_immediate(self):
        cur = self._conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            yield cur
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        finally:
            cur.close()

    def _issues_table_exists(self) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'issues'"
        ).fetchone()
        return row is not None

    def _issues_has_repo_column(self) -> bool:
        rows = self._conn.execute("PRAGMA table_info(issues)").fetchall()
        return any(str(row["name"]) == "repo" for row in rows)

    def _create_schema_v2(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS issues (
              repo TEXT NOT NULL,
              id INTEGER NOT NULL,
              title TEXT NOT NULL,
              body TEXT,
              url TEXT,
              labels_json TEXT,
              status TEXT NOT NULL DEFAULT 'pending',
              attempt_count INTEGER NOT NULL DEFAULT 0,

              lease_until TEXT,
              claimed_by TEXT,

              branch TEXT,
              pr_number INTEGER,
              pr_url TEXT,
              head_sha TEXT,

              last_error TEXT,
              last_run_dir TEXT,

              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT,
              started_at TEXT,
              completed_at TEXT,

              PRIMARY KEY (repo, id)
            );

            CREATE INDEX IF NOT EXISTS idx_issues_repo_status ON issues(repo, status);
            CREATE INDEX IF NOT EXISTS idx_issues_repo_lease ON issues(repo, lease_until);

            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT
            );
            """
        )

    def _migrate_v1_to_v2(self) -> None:
        self._conn.execute("ALTER TABLE issues RENAME TO issues_legacy_v1")
        self._create_schema_v2()
        self._conn.execute(
            """
            INSERT INTO issues (
              repo,
              id,
              title,
              body,
              url,
              labels_json,
              status,
              attempt_count,
              lease_until,
              claimed_by,
              branch,
              pr_number,
              pr_url,
              head_sha,
              last_error,
              last_run_dir,
              created_at,
              updated_at,
              started_at,
              completed_at
            )
            SELECT
              ?,
              id,
              title,
              body,
              url,
              labels_json,
              status,
              attempt_count,
              lease_until,
              claimed_by,
              branch,
              pr_number,
              pr_url,
              head_sha,
              last_error,
              last_run_dir,
              created_at,
              updated_at,
              started_at,
              completed_at
            FROM issues_legacy_v1
            """,
            (self.repo_namespace,),
        )
        self._conn.execute("DROP TABLE issues_legacy_v1")

    def _migrate_legacy_meta_keys(self) -> None:
        rows = self._conn.execute(
            "SELECT key, value FROM meta WHERE key LIKE 'done_count:%'"
        ).fetchall()
        for row in rows:
            scoped_key = self._meta_key(str(row["key"]))
            self._conn.execute(
                """
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO NOTHING
                """,
                (scoped_key, str(row["value"])),
            )

    def _migrate(self) -> None:
        version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
        if version >= _SCHEMA_VERSION:
            return

        issues_exists = self._issues_table_exists()
        if issues_exists and not self._issues_has_repo_column():
            self._migrate_v1_to_v2()
        else:
            self._create_schema_v2()

        self._migrate_legacy_meta_keys()
        self._conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
        self._conn.commit()

    def _meta_key(self, key: str) -> str:
        return f"{self.repo_namespace}:{key}"

    def upsert_polled_issues(self, issues: Iterable[dict[str, object]]) -> None:
        now = utcnow_iso()
        with self.conn:
            for issue in issues:
                self.conn.execute(
                    """
                    INSERT INTO issues (
                      repo,
                      id,
                      title,
                      body,
                      url,
                      labels_json,
                      status,
                      updated_at,
                      created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                    ON CONFLICT(repo, id) DO UPDATE SET
                      title = excluded.title,
                      body = COALESCE(excluded.body, issues.body),
                      url = excluded.url,
                      labels_json = excluded.labels_json,
                      updated_at = excluded.updated_at
                    """,
                    (
                        self.repo_namespace,
                        int(issue["id"]),
                        str(issue["title"]),
                        issue.get("body"),
                        issue.get("url"),
                        json.dumps(issue.get("labels", [])),
                        issue.get("updated_at"),
                        now,
                    ),
                )

    def update_issue_details(self, issue: dict[str, object]) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE issues
                SET title = ?,
                    body = ?,
                    url = ?,
                    labels_json = ?,
                    updated_at = ?
                WHERE repo = ?
                  AND id = ?
                """,
                (
                    issue.get("title"),
                    issue.get("body"),
                    issue.get("url"),
                    json.dumps(issue.get("labels", [])),
                    issue.get("updated_at"),
                    self.repo_namespace,
                    int(issue["id"]),
                ),
            )

    def requeue_expired_leases(self) -> int:
        now = utcnow_iso()
        with self.conn:
            cur = self.conn.execute(
                """
                UPDATE issues
                SET status = 'pending',
                    lease_until = NULL,
                    claimed_by = NULL,
                    last_error = COALESCE(last_error, 'lease expired')
                WHERE repo = ?
                  AND status = 'running'
                  AND lease_until IS NOT NULL
                  AND lease_until < ?
                """,
                (self.repo_namespace, now),
            )
            return cur.rowcount

    def claim_next_pending(
        self,
        worker_id: str,
        max_attempts: int,
        lease_seconds: int,
    ) -> IssueRecord | None:
        lease_until = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        ).isoformat(timespec="seconds").replace("+00:00", "Z")
        started_at = utcnow_iso()

        while True:
            with self._begin_immediate() as cur:
                row = cur.execute(
                    """
                    SELECT *
                    FROM issues
                    WHERE repo = ?
                      AND status = 'pending'
                      AND attempt_count < ?
                    ORDER BY COALESCE(updated_at, created_at) DESC, id ASC
                    LIMIT 1
                    """,
                    (self.repo_namespace, max_attempts),
                ).fetchone()
                if row is None:
                    return None

                claimed = self._claim_issue(
                    cur=cur,
                    issue_id=int(row["id"]),
                    started_at=started_at,
                    lease_until=lease_until,
                    worker_id=worker_id,
                )
                if claimed is not None:
                    return claimed

    def claim_pending_by_id(
        self,
        issue_id: int,
        worker_id: str,
        max_attempts: int,
        lease_seconds: int,
    ) -> IssueRecord | None:
        lease_until = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        ).isoformat(timespec="seconds").replace("+00:00", "Z")
        started_at = utcnow_iso()
        with self._begin_immediate() as cur:
            row = cur.execute(
                """
                SELECT *
                FROM issues
                WHERE repo = ?
                  AND id = ?
                  AND status = 'pending'
                  AND attempt_count < ?
                """,
                (self.repo_namespace, issue_id, max_attempts),
            ).fetchone()
            if row is None:
                return None
            return self._claim_issue(
                cur=cur,
                issue_id=int(row["id"]),
                started_at=started_at,
                lease_until=lease_until,
                worker_id=worker_id,
            )

    def _claim_issue(
        self,
        *,
        cur: sqlite3.Cursor,
        issue_id: int,
        started_at: str,
        lease_until: str,
        worker_id: str,
    ) -> IssueRecord | None:
        updated = cur.execute(
            """
            UPDATE issues
            SET status = 'running',
                started_at = ?,
                lease_until = ?,
                claimed_by = ?,
                attempt_count = attempt_count + 1
            WHERE repo = ?
              AND id = ?
              AND status = 'pending'
            """,
            (started_at, lease_until, worker_id, self.repo_namespace, issue_id),
        )
        if updated.rowcount != 1:
            return None
        claimed = cur.execute(
            "SELECT * FROM issues WHERE repo = ? AND id = ?",
            (self.repo_namespace, issue_id),
        ).fetchone()
        if claimed is None:
            return None
        return _row_to_issue(claimed)

    def mark_done(
        self,
        issue_id: int,
        pr_number: int | None,
        pr_url: str | None,
        branch: str,
        head_sha: str | None,
        run_dir: str | None,
    ) -> None:
        completed_at = utcnow_iso()
        with self.conn:
            self.conn.execute(
                """
                UPDATE issues
                SET status = 'done',
                    pr_number = ?,
                    pr_url = ?,
                    branch = ?,
                    head_sha = ?,
                    lease_until = NULL,
                    claimed_by = NULL,
                    completed_at = ?,
                    last_error = NULL,
                    last_run_dir = ?
                WHERE repo = ?
                  AND id = ?
                """,
                (pr_number, pr_url, branch, head_sha, completed_at, run_dir, self.repo_namespace, issue_id),
            )

    def mark_failed(self, issue_id: int, error: str, run_dir: str | None) -> None:
        completed_at = utcnow_iso()
        with self.conn:
            self.conn.execute(
                """
                UPDATE issues
                SET status = 'failed',
                    lease_until = NULL,
                    claimed_by = NULL,
                    completed_at = ?,
                    last_error = ?,
                    last_run_dir = ?
                WHERE repo = ?
                  AND id = ?
                """,
                (completed_at, error, run_dir, self.repo_namespace, issue_id),
            )

    def mark_timeout(self, issue_id: int, error: str, run_dir: str | None) -> None:
        completed_at = utcnow_iso()
        with self.conn:
            self.conn.execute(
                """
                UPDATE issues
                SET status = 'timeout',
                    lease_until = NULL,
                    claimed_by = NULL,
                    completed_at = ?,
                    last_error = ?,
                    last_run_dir = ?
                WHERE repo = ?
                  AND id = ?
                """,
                (completed_at, error, run_dir, self.repo_namespace, issue_id),
            )

    def mark_skipped(self, issue_id: int, reason: str, run_dir: str | None) -> None:
        completed_at = utcnow_iso()
        with self.conn:
            self.conn.execute(
                """
                UPDATE issues
                SET status = 'skipped',
                    lease_until = NULL,
                    claimed_by = NULL,
                    completed_at = ?,
                    last_error = ?,
                    last_run_dir = ?
                WHERE repo = ?
                  AND id = ?
                """,
                (completed_at, reason, run_dir, self.repo_namespace, issue_id),
            )

    def get_status_counts(self) -> dict[str, int]:
        rows = self.conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM issues
            WHERE repo = ?
            GROUP BY status
            ORDER BY status ASC
            """,
            (self.repo_namespace,),
        ).fetchall()
        return {str(row["status"]): int(row["count"]) for row in rows}

    def clear_namespace_state(self) -> tuple[int, int]:
        with self.conn:
            issues_deleted = self.conn.execute(
                "DELETE FROM issues WHERE repo = ?",
                (self.repo_namespace,),
            ).rowcount
            meta_deleted = self.conn.execute(
                "DELETE FROM meta WHERE key LIKE ?",
                (f"{self.repo_namespace}:%",),
            ).rowcount
        return issues_deleted, meta_deleted

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = ?",
            (self._meta_key(key),),
        ).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def set_meta(self, key: str, value: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (self._meta_key(key), value),
            )

    def get_daily_done_count(self, date_yyyy_mm_dd: str) -> int:
        key = f"done_count:{date_yyyy_mm_dd}"
        value = self.get_meta(key)
        if value is None:
            return 0
        try:
            return int(value)
        except ValueError:
            return 0

    def increment_daily_done_count(self, date_yyyy_mm_dd: str) -> int:
        key = self._meta_key(f"done_count:{date_yyyy_mm_dd}")
        with self._begin_immediate() as cur:
            row = cur.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            current = 0
            if row is not None:
                try:
                    current = int(row["value"])
                except (ValueError, TypeError):
                    current = 0
            current += 1
            cur.execute(
                """
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, str(current)),
            )
            return current
