import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

STATE_PATH = Path(__file__).parent.parent / "data" / "state" / "seen_urls.json"

Status = str  # "processing" | "done" | "failed" | "skipped"


class StateManager:
    def __init__(self, state_path: Path = STATE_PATH):
        self._path = state_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._state: dict = self._load()

    def _load(self) -> dict:
        if self._path.exists():
            try:
                return json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save(self):
        self._path.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def make_key(url: str, content_hash: str = "") -> str:
        return hashlib.sha256(f"{url}::{content_hash}".encode()).hexdigest()

    def is_done(self, url: str, content_hash: str = "") -> bool:
        key = self.make_key(url, content_hash)
        return self._state.get(key, {}).get("status") == "done"

    def mark_processing(self, url: str, content_hash: str = ""):
        key = self.make_key(url, content_hash)
        self._state[key] = {
            "status": "processing",
            "url": url,
            "first_seen": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def mark_done(self, url: str, content_hash: str, output_path: str):
        key = self.make_key(url, content_hash)
        entry = self._state.get(key, {})
        entry.update({
            "status": "done",
            "url": url,
            "content_hash": content_hash,
            "output_path": output_path,
            "last_checked": datetime.now(timezone.utc).isoformat(),
        })
        self._state[key] = entry
        self._save()

    def mark_failed(self, url: str, content_hash: str = "", reason: str = ""):
        key = self.make_key(url, content_hash)
        self._state[key] = {
            "status": "failed",
            "url": url,
            "reason": reason,
            "last_checked": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def mark_skipped(self, url: str, content_hash: str = "", reason: str = ""):
        key = self.make_key(url, content_hash)
        self._state[key] = {
            "status": "skipped",
            "url": url,
            "reason": reason,
            "last_checked": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def get_stuck_processing(self) -> list[dict]:
        """前回クラッシュ時に processing のまま残ったエントリを返す。"""
        return [
            v for v in self._state.values()
            if v.get("status") == "processing"
        ]
