"""
Extract data from DHIS2 MOH into bronze.dhis2_raw.
Tags all rows with source_system = 'DHIS2_MOH'.

Usage:
    python extract_moh.py --backfill 202402
    python extract_moh.py --incremental
    python extract_moh.py --all-periods
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta

from config import config
from dhis2_extractor import DHIS2Extractor
from json_storage import JSONStorage
from db_loader import DBLoader
from periods import generate_periods
import time
from pathlib import Path
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/moh_extract.log", mode="a"),
    ],
)
logger = logging.getLogger("moh_extract")


class MOHFullExtractor:
    """
    Like FullExtractor but uses MOH credentials and tags rows as DHIS2_MOH.
    """

    def __init__(self, period, batch_size=10, delay=0.5, period_type_filter=None):
        if not config.has_moh:
            raise ValueError(
                "MOH credentials missing. Set MOH_DHIS2_URL, "
                "MOH_DHIS2_USERNAME, MOH_DHIS2_PASSWORD in .env"
            )

        self.period = period
        self.batch_size = batch_size
        self.delay = delay
        self.period_type_filter = period_type_filter

        self.extractor = DHIS2Extractor(
            url=config.MOH_DHIS2_URL,
            username=config.MOH_DHIS2_USERNAME,
            password=config.MOH_DHIS2_PASSWORD,
        )
        self.storage = JSONStorage()
        self.loader = DBLoader()

        self.checkpoint_dir = Path("checkpoints_moh")
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / f"checkpoint_{period}.json"
        self.checkpoint = self._load_checkpoint()

    def _load_checkpoint(self):
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(
                f"Resuming {self.checkpoint_file.name}: "
                f"{len(data.get('completed', []))} batches done"
            )
            return data
        return {"period": self.period, "completed": [], "failed": []}

    def _save_checkpoint(self):
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(self.checkpoint, f, indent=2)

    def _batch_key(self, ds_id, ou_idx):
        return f"{ds_id}::{ou_idx}"

    def _is_done(self, ds_id, ou_idx):
        return self._batch_key(ds_id, ou_idx) in self.checkpoint["completed"]

    def _mark_done(self, ds_id, ou_idx):
        self.checkpoint["completed"].append(self._batch_key(ds_id, ou_idx))

    def run(self):
        datasets = self.extractor.get_datasets()
        if self.period_type_filter:
            datasets = [
                d for d in datasets
                if d.get("periodType") == self.period_type_filter
            ]
            logger.info(f"MOH: filtered to {len(datasets)} datasets")

        if not datasets:
            logger.warning("MOH: no datasets")
            return

        ou_meta = self.extractor.get_org_units()
        org_unit_ids = list(ou_meta.keys())
        de_meta = self.extractor.get_data_elements()

        self.loader.create_tables()

        total_batches = len(datasets) * (
            (len(org_unit_ids) + self.batch_size - 1) // self.batch_size
        )
        logger.info(
            f"MOH: {len(datasets)} datasets × {len(org_unit_ids)} org units "
            f"= ~{total_batches} batches"
        )

        processed = 0
        skipped = 0
        total_loaded = 0

        for ds in datasets:
            ds_id = ds["id"]
            ds_name = ds.get("displayName") or ds.get("name")

            for i in range(0, len(org_unit_ids), self.batch_size):
                batch_ous = org_unit_ids[i:i + self.batch_size]
                batch_index = i // self.batch_size

                if self._is_done(ds_id, batch_index):
                    skipped += 1
                    continue

                try:
                    data = self.extractor.get_data_values(
                        ds_id, batch_ous, self.period
                    )
                    values = data.get("dataValues", [])
                    if not values:
                        self._mark_done(ds_id, batch_index)
                        processed += 1
                        continue

                    json_file = self.storage.save(
                        data, prefix=f"MOH_{ds_id}_ou{batch_index}_{self.period}"
                    )

                    received, loaded = self.loader.load_data_values(
                        data, de_meta, ou_meta,
                        source_system="DHIS2_MOH",
                        batch_id=f"MOH_{self.period}_{ds_id}_{batch_index}",
                        dataset_id=ds_id,
                    )
                    total_loaded += loaded

                    self.storage.archive(json_file)
                    self._mark_done(ds_id, batch_index)
                    self._save_checkpoint()
                    processed += 1

                    logger.info(
                        f"  MOH [{processed+skipped}/{total_batches}] "
                        f"{ds_name} | OU batch {batch_index} | loaded {loaded}"
                    )

                except Exception as e:
                    logger.error(
                        f"  MOH ✗ {ds_name} | batch {batch_index}: {e}"
                    )
                    self.checkpoint["failed"].append({
                        "dataset_id": ds_id,
                        "dataset_name": ds_name,
                        "ou_index": batch_index,
                        "error": str(e),
                    })
                    self._save_checkpoint()
                    continue

                finally:
                    time.sleep(self.delay)

        logger.info("=" * 60)
        logger.info(f"MOH EXTRACTION COMPLETE: period={self.period}")
        logger.info(f"  Batches processed: {processed}")
        logger.info(f"  Batches skipped:   {skipped}")
        logger.info(f"  Total loaded:      {total_loaded}")
        logger.info("=" * 60)


# ====================================================================== #
# CLI
# ====================================================================== #
def current_period(offset=0):
    d = datetime.utcnow().replace(day=1) - timedelta(days=1)
    for _ in range(offset):
        d = d.replace(day=1) - timedelta(days=1)
    return f"{d.year}{d.month:02d}"


def main():
    import argparse
    from datetime import datetime, timedelta

    def current_period(offset=0):
        d = datetime.utcnow().replace(day=1) - timedelta(days=1)
        for _ in range(offset):
            d = d.replace(day=1) - timedelta(days=1)
        return f"{d.year}{d.month:02d}"

    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--incremental", action="store_true",
                      help="Extract last completed month")
    mode.add_argument("--backfill", metavar="YYYYMM",
                      help="Extract a single month")
    mode.add_argument("--all-periods", action="store_true",
                      help="Extract 2023-2024")
    mode.add_argument("--years", metavar="YYYY-YYYY",
                      help="Extract a year range (e.g., 2018-2022)")
    args = parser.parse_args()

    # Determine periods based on mode
    if args.incremental:
        periods = [current_period(offset=1)]
    elif args.backfill:
        periods = [args.backfill]
    elif args.all_periods:
        periods = [f"{y}{m:02d}" for y in (2023, 2024) for m in range(1, 13)]
    elif args.years:
        # Parse the year range
        try:
            start_year, end_year = map(int, args.years.split('-'))
        except ValueError:
            parser.error("--years must be in format YYYY-YYYY (e.g., 2018-2022)")
        if start_year > end_year:
            parser.error("--years start must be <= end")
        periods = [
            f"{y}{m:02d}"
            for y in range(start_year, end_year + 1)
            for m in range(1, 13)
        ]
    else:
        parser.error("No mode selected")

    logger.info(f"MOH extraction for {len(periods)} period(s)")
    logger.info(f"  Range: {periods[0]} ... {periods[-1]}")

    # Extract each period
    succeeded = 0
    failed = 0
    for i, p in enumerate(periods, 1):
        logger.info("")
        logger.info(f"[{i}/{len(periods)}] period={p}")
        try:
            fe = MOHFullExtractor(
                period=p, batch_size=10, delay=0.3,
                period_type_filter="Monthly",
            )
            fe.run()
            succeeded += 1
        except Exception as e:
            logger.exception(f"  ✗ FAILED period {p}: {e}")
            failed += 1
            # Continue with next period
            continue

    logger.info("")
    logger.info("=" * 60)
    logger.info(f"MOH BACKFILL COMPLETE")
    logger.info(f"  Succeeded: {succeeded}/{len(periods)}")
    logger.info(f"  Failed:    {failed}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
