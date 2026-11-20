# full_extractor.py
import json
import logging
import time
from datetime import datetime
from pathlib import Path

from config import config
from dhis2_extractor import DHIS2Extractor
from json_storage import JSONStorage
from db_loader import DBLoader

logger = logging.getLogger(__name__)


class FullExtractor:
    """
    Iterates all datasets × all org units for ONE period,
    with per-period checkpointing so multi-period runs resume cleanly.
    """

    def __init__(self, period, batch_size=10, delay=0.5, period_type_filter=None):
        self.period = period
        self.batch_size = batch_size
        self.delay = delay
        self.period_type_filter = period_type_filter

        self.extractor = DHIS2Extractor()
        self.storage = JSONStorage()
        self.loader = DBLoader()

        # One checkpoint file per period
        self.checkpoint_dir = Path("checkpoints")
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / f"checkpoint_{period}.json"
        self.checkpoint = self._load_checkpoint()

    # ------------------------------------------------------------------ #
    # Checkpoint
    # ------------------------------------------------------------------ #
    def _load_checkpoint(self):
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(
                f"Resuming checkpoint {self.checkpoint_file.name}: "
                f"{len(data.get('completed', []))} batches done"
            )
            return data
        logger.info(f"New checkpoint for period={self.period}")
        return {"period": self.period, "completed": [], "failed": []}

    def _save_checkpoint(self):
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(self.checkpoint, f, indent=2)

    def _batch_key(self, ds_id, ou_index):
        return f"{ds_id}::{ou_index}"

    def _is_done(self, ds_id, ou_index):
        return self._batch_key(ds_id, ou_index) in self.checkpoint["completed"]

    def _mark_done(self, ds_id, ou_index):
        self.checkpoint["completed"].append(self._batch_key(ds_id, ou_index))

    def _mark_failed(self, ds_id, ds_name, ou_index, error):
        self.checkpoint["failed"].append({
            "dataset_id": ds_id,
            "dataset_name": ds_name,
            "ou_index": ou_index,
            "error": str(error),
            "at": datetime.utcnow().isoformat(),
        })

    def reset_checkpoint(self):
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
        self.checkpoint = {"period": self.period, "completed": [], "failed": []}
        logger.info(f"Checkpoint reset for period={self.period}")

    # ------------------------------------------------------------------ #
    # Main
    # ------------------------------------------------------------------ #
    def run(self):
        # 1. Fetch datasets
        datasets = self.extractor.get_datasets()
        if self.period_type_filter:
            datasets = [
                d for d in datasets
                if d.get("periodType") == self.period_type_filter
            ]
            logger.info(f"Filtered to {len(datasets)} datasets for {self.period_type_filter}")

        if not datasets:
            logger.warning(f"No datasets to process for period={self.period}")
            return

        # 2. Fetch metadata
        ou_meta = self.extractor.get_org_units()
        org_unit_ids = list(ou_meta.keys())
        de_meta = self.extractor.get_data_elements()

        # 3. Ensure DB tables
        self.loader.create_tables()

        total_batches = len(datasets) * (
            (len(org_unit_ids) + self.batch_size - 1) // self.batch_size
        )
        logger.info(
            f"Starting full extraction: {len(datasets)} datasets × "
            f"{len(org_unit_ids)} org units = ~{total_batches} batches"
        )

        processed = 0
        skipped = 0
        total_received = 0
        total_loaded = 0

        for ds in datasets:
            ds_id = ds["id"]
            ds_name = ds.get("displayName") or ds.get("name")

            if not self._period_matches(ds.get("periodType")):
                continue

            for i in range(0, len(org_unit_ids), self.batch_size):
                batch_ous = org_unit_ids[i:i + self.batch_size]
                batch_index = i // self.batch_size

                if self._is_done(ds_id, batch_index):
                    skipped += 1
                    continue

                try:
                    logger.info(
                        f"[{processed+skipped+1}/{total_batches}] "
                        f"{ds_name} | OU batch {batch_index}"
                    )
                    data = self.extractor.get_data_values(
                        ds_id, batch_ous, self.period
                    )
                    values = data.get("dataValues", [])
                    if not values:
                        self._mark_done(ds_id, batch_index)
                        processed += 1
                        continue

                    json_file = self.storage.save(
                        data, prefix=f"{ds_id}_ou{batch_index}_{self.period}"
                    )
                    received, loaded = self.loader.load_data_values(
                        data, de_meta, ou_meta,
                        source_system="DHIS2_PIH",
                        batch_id=f"{self.period}_{ds_id}_{batch_index}",
                        dataset_id=ds_id,
                    )
                    total_received += received
                    total_loaded += loaded
                    self.storage.archive(json_file)
                    self._mark_done(ds_id, batch_index)
                    self._save_checkpoint()
                    processed += 1
                    logger.info(f"  ✓ Received {received} | Loaded {loaded}")

                except Exception as e:
                    logger.error(f"  ✗ Failed {ds_name} | batch {batch_index}: {e}")
                    self._mark_failed(ds_id, ds_name, batch_index, e)
                    self._save_checkpoint()
                    continue

                finally:
                    time.sleep(self.delay)

        logger.info("=" * 70)
        logger.info("FULL EXTRACTION COMPLETE")
        logger.info(f"Datasets processed: {len(datasets)}")
        logger.info(f"Batches processed: {processed}")
        logger.info(f"Batches skipped (resume): {skipped}")
        logger.info(f"Total received: {total_received}")
        logger.info(f"Total loaded: {total_loaded}")
        logger.info("=" * 70)

    def _period_matches(self, period_type):
        if not period_type:
            return True
        p = self.period
        pt = period_type.lower()
        if pt == "monthly" and len(p) == 6 and p.isdigit():
            return True
        if pt == "quarterly" and "q" in p.lower():
            return True
        if pt == "yearly" and len(p) == 4 and p.isdigit():
            return True
        return True
