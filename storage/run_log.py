import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

LOGS_DIR = Path(__file__).parent.parent / "data" / "run_logs"


class RunLog:
    def __init__(self, logs_dir: Path = LOGS_DIR):
        logs_dir.mkdir(parents=True, exist_ok=True)
        today = date.today().isoformat()
        self._path = logs_dir / f"{today}.jsonl"

    def append(self, event: str, **kwargs):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **kwargs,
        }
        with open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def summary(self, municipality_id: str, discovered: int, new: int,
                updated: int, skipped: int, failed: int):
        self.append(
            "scrape_complete",
            municipality=municipality_id,
            discovered=discovered,
            new=new,
            updated=updated,
            skipped=skipped,
            failed=failed,
        )

    def auto_fix(self, municipality_id: str, success: bool):
        self.append(
            "auto_fix",
            municipality=municipality_id,
            success=success,
        )
