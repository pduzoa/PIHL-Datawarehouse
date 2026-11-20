"""
Populate V2 metadata dimensions and gold.value.

Supports multi-source via `source_system_id`:
    1 = DHIS2 PIH
    2 = DHIS2 MOH

Column names match actual DB schema:
    data_element_new.name / .description
    data_set.name / .description
    disaggregation_new.name
    location.name
    period.period_str

Usage:
    python populate_v2.py --source pih
    python populate_v2.py --source moh
    python populate_v2.py --source both
"""

import argparse
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("populate_v2")


class PopulateV2:
    def __init__(self):
        self.engine = create_engine(config.db_url, pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)

    # ================================================================== #
    # 1. Periods (shared)
    # ================================================================== #
    def sync_periods(self):
        logger.info("Syncing periods...")
        s = self.Session()
        try:
            s.execute(text("""
                INSERT INTO metadata.period (period_str, period_type_id)
                SELECT DISTINCT
                    src.period,
                    CASE
                        WHEN LENGTH(src.period) = 6 AND src.period ~ '^[0-9]+$'
                            THEN (SELECT period_type_id FROM metadata.period_type WHERE code='Monthly')
                        WHEN src.period ~ '^[0-9]{4}Q[1-4]$'
                            THEN (SELECT period_type_id FROM metadata.period_type WHERE code='Quarterly')
                        WHEN LENGTH(src.period) = 4 AND src.period ~ '^[0-9]+$'
                            THEN (SELECT period_type_id FROM metadata.period_type WHERE code='Yearly')
                    END
                FROM silver.dhis2_data src
                WHERE src.period IS NOT NULL
                ON CONFLICT (period_str) DO NOTHING
            """))
            s.commit()
            logger.info("  periods: done")
        except Exception:
            s.rollback()
            logger.exception("sync_periods failed")
            raise
        finally:
            s.close()

    # ================================================================== #
    # 2. Locations
    # ================================================================== #
    def sync_locations(self, source_system_id=1, source_system="DHIS2_PIH"):
        logger.info(f"Syncing locations for {source_system}...")
        s = self.Session()
        try:
            s.execute(text("""
                INSERT INTO metadata.location
                    (source_id, name, level, source_system_id)
                SELECT DISTINCT
                    src.orgunit_id,
                    src.orgunit_name,
                    NULL::integer,
                    :src_id
                FROM silver.dhis2_data src
                WHERE src.source_system = :src
                  AND src.orgunit_id IS NOT NULL
                  AND src.orgunit_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM metadata.location l
                      WHERE l.source_id = src.orgunit_id
                        AND l.source_system_id = :src_id
                  )
            """), {"src": source_system, "src_id": source_system_id})
            s.commit()
            logger.info("  locations: done")
        except Exception:
            s.rollback()
            logger.exception("sync_locations failed")
            raise
        finally:
            s.close()

    # ================================================================== #
    # 3. Datasets (name, description)
    # ================================================================== #
    def sync_data_sets(self, source_system_id=1, source_system="DHIS2_PIH"):
        logger.info(f"Syncing data_sets for {source_system}...")
        s = self.Session()
        try:
            s.execute(text("""
                INSERT INTO metadata.data_set
                    (source_id, name, program, description, period_type_id, source_system_id)
                SELECT DISTINCT
                    src.dataset_id,
                    src.dataset_name,
                    NULL,
                    NULL,
                    (SELECT period_type_id FROM metadata.period_type WHERE code='Monthly'),
                    :src_id
                FROM silver.dhis2_data src
                WHERE src.source_system = :src
                  AND src.dataset_id IS NOT NULL
                  AND src.dataset_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM metadata.data_set ds
                      WHERE ds.source_id = src.dataset_id
                        AND ds.source_system_id = :src_id
                  )
            """), {"src": source_system, "src_id": source_system_id})
            s.commit()
            logger.info("  data_sets: done")
        except Exception:
            s.rollback()
            logger.exception("sync_data_sets failed")
            raise
        finally:
            s.close()

    # ================================================================== #
    # 4. Data elements (name, description)
    # ================================================================== #
    def sync_data_elements(self, source_system_id=1, source_system="DHIS2_PIH"):
        logger.info(f"Syncing data_elements for {source_system}...")
        s = self.Session()
        try:
            s.execute(text("""
                INSERT INTO metadata.data_element_new
                    (source_id, name, description, type_id, source_system_id)
                SELECT DISTINCT
                    src.de_id,
                    src.de_name,
                    src.de_description,
                    NULL,
                    :src_id
                FROM silver.dhis2_data src
                WHERE src.source_system = :src
                  AND src.de_id IS NOT NULL
                  AND src.de_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM metadata.data_element_new den
                      WHERE den.source_id = src.de_id
                        AND den.source_system_id = :src_id
                  )
            """), {"src": source_system, "src_id": source_system_id})
            s.commit()
            logger.info("  data_elements: done")
        except Exception:
            s.rollback()
            logger.exception("sync_data_elements failed")
            raise
        finally:
            s.close()

    # ================================================================== #
    # 5. Disaggregations (name)
    # ================================================================== #
    def sync_disaggregations(self, source_system_id=1, source_system="DHIS2_PIH"):
        logger.info(f"Syncing disaggregations for {source_system}...")
        s = self.Session()
        try:
            s.execute(text("""
                INSERT INTO metadata.disaggregation_new
                    (source_id, name, type_id, source_system_id)
                SELECT DISTINCT
                    src.disaggregation_uid,
                    src.disaggregation_uid,
                    'CategoryOptionCombo',
                    :src_id
                FROM silver.dhis2_data src
                WHERE src.source_system = :src
                  AND src.disaggregation_uid IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM metadata.disaggregation_new dn
                      WHERE dn.source_id = src.disaggregation_uid
                        AND dn.source_system_id = :src_id
                  )
            """), {"src": source_system, "src_id": source_system_id})
            s.commit()
            logger.info("  disaggregations: done")
        except Exception:
            s.rollback()
            logger.exception("sync_disaggregations failed")
            raise
        finally:
            s.close()

    # ================================================================== #
    # 6. Dataset ↔ DE bridge
    # ================================================================== #
    def sync_dataset_to_de(self, source_system_id=1, source_system="DHIS2_PIH"):
        logger.info(f"Syncing dataset↔de bridge for {source_system}...")
        s = self.Session()
        try:
            s.execute(text("""
                INSERT INTO metadata.data_set_to_data_element
                    (data_set_id, data_element_id, source_system_id)
                SELECT DISTINCT
                    ds.data_set_id,
                    de.data_element_id,
                    :src_id
                FROM silver.dhis2_data src
                JOIN metadata.data_set ds
                    ON ds.source_id = src.dataset_id
                   AND ds.source_system_id = :src_id
                JOIN metadata.data_element_new de
                    ON de.source_id = src.de_id
                   AND de.source_system_id = :src_id
                WHERE src.source_system = :src
                  AND src.dataset_id IS NOT NULL
                  AND src.de_id IS NOT NULL
                ON CONFLICT DO NOTHING
            """), {"src": source_system, "src_id": source_system_id})
            s.commit()
            logger.info("  dataset↔de bridge: done")
        except Exception:
            s.rollback()
            logger.exception("sync_dataset_to_de failed")
            raise
        finally:
            s.close()

    # ================================================================== #
    # 7. Validity
    # ================================================================== #
    def sync_validity(self, source_system_id=1, source_system="DHIS2_PIH"):
        logger.info(f"Syncing validity for {source_system}...")
        s = self.Session()
        try:
            s.execute(text("""
                INSERT INTO metadata.validity (data_set_id, is_valid, valid_from)
                SELECT data_set_id, TRUE, '2015-01-01'
                FROM metadata.data_set
                WHERE source_system_id = :src_id
                ON CONFLICT DO NOTHING
            """), {"src_id": source_system_id})
            s.commit()
            logger.info("  validity: done")
        except Exception:
            s.rollback()
            logger.exception("sync_validity failed")
            raise
        finally:
            s.close()

    # ================================================================== #
    # 8. gold.value
    # ================================================================== #
    def sync_gold_value(self, source_system_id=1, source_system="DHIS2_PIH"):
        logger.info(f"Populating gold.value for {source_system}...")
        s = self.Session()
        try:
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
            logger.info(f"  gold.value ({source_system}): {result.rowcount} rows")
            return result.rowcount
        except Exception:
            s.rollback()
            logger.exception(f"gold.value sync failed for {source_system}")
            raise
        finally:
            s.close()

    # ================================================================== #
    # Orchestration
    # ================================================================== #
    def run_for_source(self, source_system_id, source_system):
        self.sync_periods()
        self.sync_locations(source_system_id, source_system)
        self.sync_data_sets(source_system_id, source_system)
        self.sync_data_elements(source_system_id, source_system)
        self.sync_disaggregations(source_system_id, source_system)
        self.sync_dataset_to_de(source_system_id, source_system)
        self.sync_validity(source_system_id, source_system)
        self.sync_gold_value(source_system_id, source_system)
        self.report()

    def report(self):
        logger.info("=" * 60)
        logger.info("V2 population summary")
        logger.info("=" * 60)
        s = self.Session()
        try:
            rows = s.execute(text("""
                SELECT
                    (SELECT COUNT(*) FROM metadata.period)             AS periods,
                    (SELECT COUNT(*) FROM metadata.location)           AS locations,
                    (SELECT COUNT(*) FROM metadata.data_set)           AS data_sets,
                    (SELECT COUNT(*) FROM metadata.data_element_new)   AS data_elements,
                    (SELECT COUNT(*) FROM metadata.disaggregation_new) AS disaggregations,
                    (SELECT COUNT(*) FROM gold.value)                  AS fact_rows
            """)).fetchone()

            logger.info(f"  Periods:                 {rows[0]:>10}")
            logger.info(f"  Locations:               {rows[1]:>10}")
            logger.info(f"  Data sets:               {rows[2]:>10}")
            logger.info(f"  Data elements:           {rows[3]:>10}")
            logger.info(f"  Disaggregations:         {rows[4]:>10}")
            logger.info(f"  Fact rows (gold.value):  {rows[5]:>10}")
            logger.info("=" * 60)

            logger.info("gold.value by source:")
            for src_id, src_name, cnt in s.execute(text("""
                SELECT v.source_system_id,
                       COALESCE(ss.source_name, 'UNKNOWN'),
                       COUNT(*)
                FROM gold.value v
                LEFT JOIN metadata.source_system ss
                    ON ss.source_system_id = v.source_system_id
                GROUP BY 1, 2
                ORDER BY 1
            """)).fetchall():
                logger.info(f"  {src_name:<15} {cnt:>10}")
            logger.info("=" * 60)
        finally:
            s.close()


# ====================================================================== #
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["pih", "moh", "both"], default="pih")
    args = parser.parse_args()

    if args.source in ("pih", "both"):
        PopulateV2().run_for_source(1, "DHIS2_PIH")

    if args.source in ("moh", "both"):
        if not config.has_moh:
            logger.warning("MOH credentials missing — skipping")
        else:
            PopulateV2().run_for_source(2, "DHIS2_MOH")

    logger.info("V2 population complete")
