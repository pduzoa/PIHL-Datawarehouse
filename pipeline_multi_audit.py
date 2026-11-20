"""
Audit helpers for the multi-source pipeline.

Writes RUNNING/SUCCESS/FAILED rows to metadata.etl_batch
and updates etl.pipeline_control. Never raises — audit failures
should never break the pipeline itself.
"""

import logging
from sqlalchemy import text

logger = logging.getLogger("pipeline_audit")


def start_batch(session_factory, pipeline_name, layer, source_system_id=1):
    """Insert a RUNNING row into metadata.etl_batch. Returns batch_id or None."""
    session = session_factory()
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
        logger.info(f"  [batch {batch_id}] {layer} started")
        return batch_id
    except Exception:
        session.rollback()
        logger.warning(f"Could not start batch for {layer}")
        return None
    finally:
        session.close()


def end_batch(session_factory, batch_id, status,
              extracted=0, inserted=0, updated=0,
              rejected=0, error=None):
    """Mark a batch complete. No-op if batch_id is None."""
    if batch_id is None:
        return
    session = session_factory()
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
        logger.info(f"  [batch {batch_id}] {status}")
    except Exception:
        session.rollback()
        logger.warning(f"Could not end batch {batch_id}")
    finally:
        session.close()


def update_control(session_factory, pipeline_name, period,
                   status="SUCCESS", source_system="DHIS2_PIH"):
    """Upsert a row in etl.pipeline_control."""
    session = session_factory()
    try:
        session.execute(text("""
            INSERT INTO etl.pipeline_control
                (pipeline_name, source_system, last_successful_run,
                 last_extracted_period, last_run_status, updated_at)
            VALUES (:pipe, :src, now(), :period, :status, now())
            ON CONFLICT (pipeline_name) DO UPDATE
                SET last_successful_run   = CASE
                        WHEN EXCLUDED.last_run_status = 'SUCCESS'
                        THEN EXCLUDED.last_successful_run
                        ELSE etl.pipeline_control.last_successful_run
                    END,
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
        logger.info(f"  pipeline_control updated: {pipeline_name} = {status}")
    except Exception:
        session.rollback()
        logger.warning("Could not update pipeline_control")
    finally:
        session.close()
