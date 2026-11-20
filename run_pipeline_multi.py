"""
Multi-source pipeline orchestrator.

Runs both PIH and MOH through the full medallion pipeline with
audit logging at every stage.

Usage:
    python run_pipeline_multi.py --incremental
    python run_pipeline_multi.py --backfill 202402
    python run_pipeline_multi.py --all-periods
    python run_pipeline_multi.py --incremental --source pih
    python run_pipeline_multi.py --incremental --source moh
    python run_pipeline_multi.py --incremental --skip-legacy
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import config
from db_loader import DBLoader
from populate_v2 import PopulateV2
from populate_v2_bridges import BridgePopulator
import pipeline_multi_audit as audit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline_multi.log", mode="a"),
    ],
)
logger = logging.getLogger("pipeline_multi")


# ====================================================================== #
# Configuration
# ====================================================================== #
class SourceConfig:
    """Encapsulates source-specific details."""
    def __init__(self, source_system_id, source_name, extract_fn, enabled=True):
        self.source_system_id = source_system_id
        self.source_name = source_name
        self.extract_fn = extract_fn
        self.enabled = enabled


def current_period(offset=0):
    """Return YYYYMM for current month minus offset."""
    d = datetime.utcnow().replace(day=1) - timedelta(days=1)
    for _ in range(offset):
        d = d.replace(day=1) - timedelta(days=1)
    return f"{d.year}{d.month:02d}"


# ====================================================================== #
# Pipeline
# ====================================================================== #
class MultiPipeline:
    def __init__(self, sessions):
        self.sessions = sessions

    def _run_stage(self, pipeline_name, layer, source_system_id, fn):
        """Run a stage with audit logging. Returns True on success."""
        batch_id = audit.start_batch(
            self.sessions, pipeline_name, layer, source_system_id
        )
        try:
            result = fn()
            audit.end_batch(
                self.sessions, batch_id, "SUCCESS",
                inserted=result if isinstance(result, int) else 0,
            )
            return True
        except Exception as e:
            audit.end_batch(
                self.sessions, batch_id, "FAILED", error=str(e)
            )
            logger.exception(f"  ✗ {layer} failed: {e}")
            return False

    # ------------------------------------------------------------------ #
    def run_source(self, source_cfg, periods, pipeline_name):
        """Run the full pipeline for one source."""
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"SOURCE: {source_cfg.source_name}")
        logger.info(f"Periods: {periods}")
        logger.info("=" * 70)

        sid = source_cfg.source_system_id
        src = source_cfg.source_name

        # ---- Stage 1: Extract ----
        def extract():
            source_cfg.extract_fn(periods)
            return len(periods)
        if not self._run_stage(pipeline_name, f"extract_{src.lower()}", sid, extract):
            return False

        # ---- Stage 2: Sync silver ----
        def sync_silver():
            loader = DBLoader()
            n = loader.sync_silver(source_system=src)
            return n or 0
        if not self._run_stage(pipeline_name, f"silver_{src.lower()}", sid, sync_silver):
            return False

        # ---- Stage 3: Metadata V2 ----
        def populate_metadata():
            p = PopulateV2()
            p.sync_locations(sid, src)
            p.sync_data_sets(sid, src)
            p.sync_data_elements(sid, src)
            p.sync_disaggregations(sid, src)
            p.sync_dataset_to_de(sid, src)
            p.sync_validity(sid, src)
            return 0
        if not self._run_stage(pipeline_name, f"metadata_{src.lower()}", sid, populate_metadata):
            return False

        # ---- Stage 4: Bridges V2 ----
        def populate_bridges():
            b = BridgePopulator(sid, src)
            b.sync_category_combos()
            b.sync_de_to_disagg()
            b.sync_combo_options()
            return 0
        if not self._run_stage(pipeline_name, f"bridges_{src.lower()}", sid, populate_bridges):
            return False

        # ---- Stage 5: Gold value V2 ----
        def populate_gold():
            p = PopulateV2()
            n = p.sync_gold_value(sid, src)
            return n or 0
        if not self._run_stage(pipeline_name, f"gold_value_{src.lower()}", sid, populate_gold):
            return False

        # ---- All stages succeeded ----
        audit.update_control(
            self.sessions, pipeline_name, periods[-1] if periods else "N/A",
            status="SUCCESS", source_system=src,
        )
        logger.info(f"✓ {source_cfg.source_name}: COMPLETE")
        return True


# ====================================================================== #
# Extraction wrappers
# ====================================================================== #
def extract_pih(periods):
    from full_extractor import FullExtractor
    for p in periods:
        logger.info(f"  PIH extracting period={p}")
        FullExtractor(
            period=p, batch_size=10, delay=0.3,
            period_type_filter="Monthly",
        ).run()


def extract_moh(periods):
    from extract_moh import MOHFullExtractor
    for p in periods:
        logger.info(f"  MOH extracting period={p}")
        MOHFullExtractor(
            period=p, batch_size=10, delay=0.3,
            period_type_filter="Monthly",
        ).run()


# ====================================================================== #
# Main
# ====================================================================== #
def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--incremental", action="store_true")
    mode.add_argument("--backfill", metavar="YYYYMM")
    mode.add_argument("--all-periods", action="store_true")
    parser.add_argument("--source", choices=["pih", "moh", "all"], default="all")
    args = parser.parse_args()

    if args.incremental:
        periods = [current_period(offset=1)]
    elif args.backfill:
        periods = [args.backfill]
    else:
        periods = [f"{y}{m:02d}" for y in (2023, 2024) for m in range(1, 13)]

    # Build source list
    sources = []
    if args.source in ("pih", "all"):
        sources.append(SourceConfig(1, "DHIS2_PIH", extract_pih))
    if args.source in ("moh", "all"):
        if not config.has_moh:
            logger.warning("MOH credentials missing — skipping MOH")
        else:
            sources.append(SourceConfig(2, "DHIS2_MOH", extract_moh))

    if not sources:
        logger.error("No sources to run")
        sys.exit(1)

    # DB sessions
    engine = create_engine(config.db_url, pool_pre_ping=True)
    sessions = sessionmaker(bind=engine)

    pipeline_name = (
        f"{'incremental' if args.incremental else 'backfill' if args.backfill else 'full'}"
        f"_{periods[0] if len(periods) == 1 else 'range'}"
    )

    logger.info("=" * 70)
    logger.info(f"PIPELINE START: {pipeline_name}")
    logger.info(f"Sources: {', '.join(s.source_name for s in sources)}")
    logger.info(f"Periods: {periods[0]} ... {periods[-1]} ({len(periods)} total)")
    logger.info("=" * 70)

    pipeline = MultiPipeline(sessions)
    results = {}

    for source in sources:
        try:
            ok = pipeline.run_source(source, periods, pipeline_name)
            results[source.source_name] = ok
        except Exception:
            logger.exception(f"{source.source_name}: unhandled exception")
            results[source.source_name] = False
            audit.update_control(
                sessions, pipeline_name,
                periods[-1] if periods else "N/A",
                status="FAILED", source_system=source.source_name,
            )

    # Final summary
    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 70)
    for src_name, ok in results.items():
        status = "✓ SUCCESS" if ok else "✗ FAILED"
        logger.info(f"  {src_name:<15} {status}")
    logger.info("=" * 70)

    all_ok = all(results.values())
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
