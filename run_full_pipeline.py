"""
End-to-end pipeline: extract → metadata → silver → gold.

Logs to metadata.etl_batch / etl.pipeline_log and updates etl.pipeline_control.

Run modes:
    --incremental          → extract last completed month only (for cron)
    --backfill YYYYMM      → extract a specific month
    --all-periods          → extract 2023-2024 full range
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta

from sqlalchemy import text

from dhis2_extractor import DHIS2Extractor
from db_loader import DBLoader
from gold_loader import GoldLoader
from full_extractor import FullExtractor


logger = logging.getLogger("pipeline")


# ===================================================================== #
# Helpers
# ===================================================================== #
def current_period(offset_months=0):
    """Return YYYYMM for current month minus offset_months."""
    d = datetime.utcnow().replace(day=1) - timedelta(days=1)
    for _ in range(offset_months):
        d = d.replace(day=1) - timedelta(days=1)
    return f"{d.year}{d.month:02d}"


def log_batch_start(loader, pipeline_name, layer, source_system_id=1):
    """
    Insert a RUNNING row into metadata.etl_batch.
    Returns batch_id (or None if the log insert fails for any reason).
    """
    session = loader.Session()
    try:
        result = session.execute(text("""
            INSERT INTO metadata.etl_batch
                (source_system_id, pipeline_name, layer, start_time, status)
            VALUES (:src, :pipe, :layer, now(), 'RUNNING')
            RETURNING batch_id
        """), {
            "src": source_system_id,
            "pipe": pipeline_name,
            "layer": layer,
        })
        batch_id = result.scalar()
        session.commit()
        logger.info(f"Batch {batch_id} started ({layer})")
        return batch_id
    except Exception as e:
        session.rollback()
        logger.warning(f"Could not start batch log for {layer}: {e}")
        return None
    finally:
        session.close()


def log_batch_end(loader, batch_id, status,
                  extracted=0, inserted=0, updated=0,
                  rejected=0, error=None):
    """Mark a batch as complete. Safe no-op if batch_id is None."""
    if batch_id is None:
        return
    session = loader.Session()
    try:
        session.execute(text("""
            UPDATE metadata.etl_batch
            SET end_time = now(),
                status = :status,
                records_extracted = :ext,
                records_inserted = :ins,
                records_updated = :upd,
                records_rejected = :rej,
                error_message = :err
            WHERE batch_id = :id
        """), {
            "id": batch_id,
            "status": status,
            "ext": extracted,
            "ins": inserted,
            "upd": updated,
            "rej": rejected,
            "err": error,
        })
        session.commit()
        logger.info(f"Batch {batch_id} ended ({status})")
    except Exception as e:
        session.rollback()
        logger.warning(f"Could not end batch {batch_id}: {e}")
    finally:
        session.close()


def update_pipeline_control(loader, pipeline_name, period,
                            status="SUCCESS", source_system="DHIS2_PIH"):
    """Upsert a row in etl.pipeline_control. Never raises."""
    session = loader.Session()
    try:
        session.execute(text("""
            INSERT INTO etl.pipeline_control
                (pipeline_name, source_system, last_successful_run,
                 last_extracted_period, last_run_status, updated_at)
            VALUES (:pipe, :src, now(), :period, :status, now())
            ON CONFLICT (pipeline_name) DO UPDATE
                SET last_successful_run   = EXCLUDED.last_successful_run,
                    last_extracted_period = EXCLUDED.last_extracted_period,
                    last_run_status       = EXCLUDED.last_run_status,
                    updated_at            = now()
        """), {
            "pipe": pipeline_name,
            "src": source_system,
            "period": period,
            "status": status,
        })
        session.commit()
        logger.info(f"pipeline_control updated: {pipeline_name} = {status}")
    except Exception as e:
        session.rollback()
        logger.warning(f"Could not update pipeline_control: {e}")
    finally:
        session.close()


# ===================================================================== #
# The pipeline
# ===================================================================== #
def run(periods, label):
    """
    Run the entire pipeline for a list of YYYYMM periods:
      1. Extract      → bronze.dhis2_raw
      2. Metadata     → metadata.*
      3. Silver       → silver.dhis2_data
      4. Gold         → gold.fact_health_data
    """
    loader = DBLoader()

    # ---------- 1. Extract → Bronze ----------
    extract_batch = log_batch_start(loader, label, "extract")
    total_extracted = 0
    try:
        for period in periods:
            logger.info(f"Extracting period={period}")
            fe = FullExtractor(
                period=period,
                batch_size=10,
                delay=0.3,
                period_type_filter="Monthly",
            )
            fe.run()
            total_extracted += 1

        log_batch_end(loader, extract_batch, "SUCCESS",
                      extracted=total_extracted)
    except Exception as e:
        log_batch_end(loader, extract_batch, "FAILED", error=str(e))
        update_pipeline_control(loader, label,
                                period=periods[-1] if periods else "N/A",
                                status="FAILED")
        raise

    # ---------- 2. Metadata ----------
    meta_batch = log_batch_start(loader, label, "metadata")
    try:
        loader.sync_metadata(source_system_id=1)
        log_batch_end(loader, meta_batch, "SUCCESS")
    except Exception as e:
        log_batch_end(loader, meta_batch, "FAILED", error=str(e))
        update_pipeline_control(loader, label,
                                period=periods[-1] if periods else "N/A",
                                status="FAILED")
        raise

    # ---------- 3. Silver ----------
    silver_batch = log_batch_start(loader, label, "silver")
    try:
        n = loader.sync_silver(source_system="DHIS2_PIH")
        log_batch_end(loader, silver_batch, "SUCCESS", inserted=n or 0)
    except Exception as e:
        log_batch_end(loader, silver_batch, "FAILED", error=str(e))
        update_pipeline_control(loader, label,
                                period=periods[-1] if periods else "N/A",
                                status="FAILED")
        raise

    # ---------- 4. Gold ----------
    gold_batch = log_batch_start(loader, label, "gold")
    try:
        gl = GoldLoader()
        n = gl.sync_fact()
        log_batch_end(loader, gold_batch, "SUCCESS", inserted=n or 0)
    except Exception as e:
        log_batch_end(loader, gold_batch, "FAILED", error=str(e))
        update_pipeline_control(loader, label,
                                period=periods[-1] if periods else "N/A",
                                status="FAILED")
        raise

    # ---------- Done ----------
    update_pipeline_control(
        loader, label,
        period=periods[-1] if periods else "N/A",
        status="SUCCESS",
    )
    logger.info(f"Pipeline {label} complete for {len(periods)} period(s)")


# ===================================================================== #
# CLI
# ===================================================================== #
def main():
    parser = argparse.ArgumentParser(
        description="End-to-end DHIS2 → PostgreSQL pipeline",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--incremental", action="store_true",
                      help="Extract last completed month only (for cron)")
    mode.add_argument("--backfill", metavar="YYYYMM",
                      help="Extract a specific month")
    mode.add_argument("--all-periods", action="store_true",
                      help="Extract 2023-2024 full range")

    args = parser.parse_args()

    if args.incremental:
        period = current_period(offset_months=1)  # last completed month
        logger.info(f"Incremental run for period={period}")
        run([period], f"incremental_{period}")

    elif args.backfill:
        logger.info(f"Backfill run for period={args.backfill}")
        run([args.backfill], f"backfill_{args.backfill}")

    elif args.all_periods:
        periods = [f"{y}{m:02d}" for y in (2023, 2024) for m in range(1, 13)]
        logger.info(f"Full run for {len(periods)} periods")
        run(periods, "full_backfill")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("logs/pipeline.log",
                                mode="a", encoding="utf-8"),
        ],
    )
    main()
