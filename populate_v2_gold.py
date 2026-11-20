"""
Populate gold.value combining all sources.

Usage:
    python populate_v2_gold.py --source pih
    python populate_v2_gold.py --source moh
    python populate_v2_gold.py --all       # both
"""

import argparse
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("populate_v2_gold")


class GoldValueLoader:
    def __init__(self):
        self.engine = create_engine(config.db_url, pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)

    def sync_source(self, source_system_id, source_system):
        """Reload gold.value for a single source (delete + insert)."""
        logger.info(f"Populating gold.value for {source_system}...")
        s = self.Session()
        try:
            # Delete existing rows for this source
            s.execute(
                text("DELETE FROM gold.value WHERE source_system_id = :src_id"),
                {"src_id": source_system_id},
            )

            result = s.execute(text("""
                INSERT INTO gold.value (
                    value_numeric, value_text, value_type,
                    location_id, data_set_id, data_element_id,
                    period_id, disaggregation_id,
                    source_system_id, last_updated
                )
                SELECT
                    src.value,
                    NULL,
                    'numeric',
                    l.location_id,
                    ds.data_set_id,
                    de.data_element_id,
                    p.period_id,
                    dis.disaggregation_id,
                    :src_id,
                    src.last_updated
                FROM silver.dhis2_data src
                LEFT JOIN metadata.location l
                    ON l.source_id = src.orgunit_id
                   AND l.source_system_id = :src_id
                LEFT JOIN metadata.data_set ds
                    ON ds.source_id = src.dataset_id
                   AND ds.source_system_id = :src_id
                LEFT JOIN metadata.data_element_new de
                    ON de.source_id = src.de_id
                   AND de.source_system_id = :src_id
                LEFT JOIN metadata.period p
                    ON p.period_str = src.period
                LEFT JOIN metadata.disaggregation_new dis
                    ON dis.source_id = src.disaggregation_uid
                   AND dis.source_system_id = :src_id
                WHERE src.source_system = :src
                  AND src.value IS NOT NULL
            """), {"src": source_system, "src_id": source_system_id})
            s.commit()
            logger.info(f"  Inserted {result.rowcount} rows for {source_system}")
            return result.rowcount
        except Exception:
            s.rollback()
            logger.exception(f"Failed for {source_system}")
            raise
        finally:
            s.close()

    def report(self):
        s = self.Session()
        try:
            rows = s.execute(text("""
                SELECT source_system_id, COUNT(*)
                FROM gold.value
                GROUP BY source_system_id
                ORDER BY source_system_id
            """)).fetchall()
            logger.info("=" * 60)
            logger.info("gold.value by source:")
            logger.info("=" * 60)
            for src_id, count in rows:
                src_name = {1: "DHIS2_PIH", 2: "DHIS2_MOH"}.get(src_id, f"src_{src_id}")
                logger.info(f"  {src_name:<15} {count:>10}")
            total = sum(r[1] for r in rows)
            logger.info(f"  {'TOTAL':<15} {total:>10}")
            logger.info("=" * 60)
        finally:
            s.close()


# ====================================================================== #
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["pih", "moh", "all"], default="pih")
    args = parser.parse_args()

    loader = GoldValueLoader()

    if args.source in ("pih", "all"):
        loader.sync_source(1, "DHIS2_PIH")
    if args.source in ("moh", "all"):
        loader.sync_source(2, "DHIS2_MOH")

    loader.report()
