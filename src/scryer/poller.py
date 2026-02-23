from __future__ import annotations

import logging

from .config import Config
from .db import Database
from .gh import GhClient


class Poller:
    def __init__(self, config: Config, db: Database, gh: GhClient):
        self.config = config
        self.db = db
        self.gh = gh
        self.log = logging.getLogger(__name__)

    def poll_and_upsert(self) -> int:
        raw_issues = self.gh.list_open_issues(self.config.trigger_label, limit=100)
        payload: list[dict[str, object]] = []
        for issue in raw_issues:
            labels = [
                str(label.get("name"))
                for label in issue.get("labels", [])
                if isinstance(label, dict) and label.get("name")
            ]
            payload.append(
                {
                    "id": int(issue["number"]),
                    "title": str(issue["title"]),
                    "body": None,
                    "url": issue.get("url"),
                    "labels": labels,
                    "updated_at": issue.get("updatedAt"),
                }
            )

        self.db.upsert_polled_issues(payload)
        self.log.info("poll complete fetched=%s", len(payload))
        return len(payload)

