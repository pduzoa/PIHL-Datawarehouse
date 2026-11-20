#!/usr/bin/env python3
"""
DHIS2 → JSON → PostgreSQL Pipeline

Usage examples:
    # Single dataset + org units (original mode)
    python main.py --dataset <UID> --orgunits <UID1>,<UID2> --period 202401

    # Full extraction: all datasets × all org units
    python main.py --all --period 202401 --period-type Monthly

    # Resume a previous full extraction (checkpoint file is used automatically)
    python main.py --all --period 202401 --period-type Monthly

    # Reset checkpoint and start fresh
    python main.py --all --period 202401 --period-type Monthly --reset

    # Increase batch size and reduce delay
    python main.py --all --period 202401 --batch-size 20 --delay 0.2
"""

import argparse
import logging
import sys
import traceback

from config import config
from dhis2_extractor import DHIS2Extractor
from json_storage import JSONStorage
from db_loader import DBLoader
from full_extractor import FullExtractor


# ---------------------------------------------------------------------- #
# Logging
# ---------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/pipeline.log", mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("pipeline")


# ---------------------------------------------------------------------- #
# Single-run pipeline (original)
# ---------------------------------------------------------------------- #
def run_pipeline(dataset_id, org_unit_ids, period, archive=True):
    """
    Extract one dataset for a set of org units and a single period.

    Args:
        dataset_id: DHIS2 dataset UID
        org_unit_ids: list of org unit UIDs (or a single UID string)
        period: DHIS2 period string (e.g. '202401')
        archive: if True, move JSON to json_archive/ after load;
                 if False, delete the temp JSON file.

    Returns:
        True on success, False on failure.
    """
    extractor = DHIS2Extractor()
    storage = JSONStorage()
    loader = DBLoader()

    json_file = None
    try:
        # 1. Ensure tables exist
        loader.create_tables()

        # 2. Metadata (fetched once per run)
        logger.info("Fetching metadata...")
        de_meta = extractor.get_data_elements()
        ou_meta = extractor.get_org_units()

        # 3. Extract from DHIS2
        data = extractor.get_data_values(dataset_id, org_unit_ids, period)

        # 4. Save temporary JSON
        json_file = storage.save(data, prefix=f"ds_{dataset_id}")

        # 5. Load into PostgreSQL
        received, loaded = loader.load_data_values(data, de_meta, ou_meta)

        # 6. Audit log
        loader.log_extraction(
            dataset_id, period, org_unit_ids,
            json_file, received, loaded, "SUCCESS",
        )

        # 7. Archive or delete temp JSON
        if archive:
            storage.archive(json_file)
        else:
            storage.delete(json_file)

        logger.info(f"Pipeline complete: {loaded}/{received} records loaded")
        return True

    except Exception as e:
        err = traceback.format_exc()
        logger.error(f"Pipeline failed: {e}\n{err}")
        try:
            loader.log_extraction(
                dataset_id, period, org_unit_ids,
                json_file or "", 0, 0, "FAILED", str(e),
            )
        except Exception:
            logger.exception("Failed to write failure log")
        return False


# ---------------------------------------------------------------------- #
# CLI
# ---------------------------------------------------------------------- #
def _cli():
    parser = argparse.ArgumentParser(
        description="DHIS2 → JSON → PostgreSQL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Mode selection
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--all",
        action="store_true",
        help="Run full extraction: all datasets × all org units (with resume support)",
    )
    mode.add_argument(
        "--dataset",
        help="Single dataset UID (requires --orgunits and --period)",
    )

    # Common options
    parser.add_argument(
        "--period",
        required=True,
        help="DHIS2 period, e.g. 202401, 2024Q1, 2024",
    )

    # Single-dataset mode options
    parser.add_argument(
        "--orgunits",
        help="Comma-separated org unit UIDs (only for --dataset mode)",
    )

    # Full-extraction options
    parser.add_argument(
        "--period-type",
        default=None,
        help="Filter datasets by periodType (Monthly, Quarterly, Yearly, ...). "
             "Only used with --all.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Org units per API request in --all mode (default: 10)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds between requests in --all mode (default: 0.5)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset the checkpoint file and start fresh (only for --all)",
    )

    # Single-dataset-only options
    parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Delete temp JSON instead of archiving (only for --dataset mode)",
    )

    args = parser.parse_args()

    # ---------------------------------------------------------------- #
    # Dispatch
    # ---------------------------------------------------------------- #
    if args.all:
        if args.dataset or args.orgunits:
            parser.error("--all cannot be combined with --dataset or --orgunits")
        if args.no_archive:
            parser.error("--no-archive is only valid with --dataset")

        logger.info(
            f"Starting FULL extraction: period={args.period}, "
            f"period_type={args.period_type or 'any'}, "
            f"batch_size={args.batch_size}, delay={args.delay}"
        )

        fe = FullExtractor(
            period=args.period,
            batch_size=args.batch_size,
            delay=args.delay,
            period_type_filter=args.period_type,
        )

        if args.reset:
            fe.reset_checkpoint()

        fe.run()

    else:
        # Single dataset mode
        if not args.orgunits:
            parser.error("--dataset mode requires --orgunits")
        if args.reset:
            parser.error("--reset is only valid with --all")

        org_unit_ids = [ou.strip() for ou in args.orgunits.split(",") if ou.strip()]
        if not org_unit_ids:
            parser.error("--orgunits must contain at least one UID")

        logger.info(
            f"Starting SINGLE extraction: dataset={args.dataset}, "
            f"org_units={len(org_unit_ids)}, period={args.period}"
        )

        success = run_pipeline(
            dataset_id=args.dataset,
            org_unit_ids=org_unit_ids,
            period=args.period,
            archive=not args.no_archive,
        )
        sys.exit(0 if success else 1)


# ---------------------------------------------------------------------- #
# Entry point
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    _cli()
