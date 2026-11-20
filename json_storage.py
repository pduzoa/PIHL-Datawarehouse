import json
import os
import logging
from datetime import datetime
from pathlib import Path

from config import config

logger = logging.getLogger(__name__)


class JSONStorage:
    def __init__(self, temp_dir=None):
        self.temp_dir = Path(temp_dir or config.JSON_TEMP_DIR)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def save(self, data, prefix="dhis2"):
        """Save dict/list to a timestamped JSON file and return the path."""
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filepath = self.temp_dir / f"{prefix}_{ts}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"Saved JSON file: {filepath} ({filepath.stat().st_size} bytes)")
        return filepath

    def load(self, filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def archive(self, filepath, archive_dir="json_archive"):
        """Move processed file to archive instead of deleting."""
        archive_path = Path(archive_dir)
        archive_path.mkdir(parents=True, exist_ok=True)
        dest = archive_path / Path(filepath).name
        os.replace(filepath, dest)
        logger.info(f"Archived: {filepath} → {dest}")
        return dest

    def delete(self, filepath):
        try:
            os.remove(filepath)
            logger.info(f"Deleted temp file: {filepath}")
        except FileNotFoundError:
            pass
