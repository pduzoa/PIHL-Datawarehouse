"""
Populate V2 bridge tables, scoped per source.
  - category combos → disaggregation_new (type_id = 'CategoryCombo')
  - data_element_to_disaggregation (DE → CC)
  - combo_option (explode disaggregation names)

Usage:
    python populate_v2_bridges.py --source pih    # source_system_id = 1
    python populate_v2_bridges.py --source moh    # source_system_id = 2
"""

import argparse
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config import config
from dhis2_extractor import DHIS2Extractor

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("populate_v2_bridges")


class BridgePopulator:
    def __init__(self, source_system_id=1, source_system="DHIS2_PIH"):
        self.engine = create_engine(config.db_url, pool_pre_ping=True)
        self.Session = sessionmaker(bind=self.engine)
        self.source_system_id = source_system_id
        self.source_system = source_system

        if source_system == "DHIS2_MOH":
            if not config.has_moh:
                raise ValueError("MOH credentials missing in .env")
            self.extractor = DHIS2Extractor(
                url=config.MOH_DHIS2_URL,
                username=config.MOH_DHIS2_USERNAME,
                password=config.MOH_DHIS2_PASSWORD,
            )
        else:
            self.extractor = DHIS2Extractor()

    # ------------------------------------------------------------------ #
    def sync_category_combos(self):
        logger.info(f"Syncing CCs for source={self.source_system}...")
        combos = self.extractor.get_category_combos()
        logger.info(f"  Fetched {len(combos)} CCs from DHIS2")

        s = self.Session()
        try:
            inserted = 0
            for cc in combos:
                cc_id = cc.get("id")
                cc_name = cc.get("displayName") or cc.get("name")
                if not cc_id:
                    continue
                result = s.execute(text("""
                    INSERT INTO metadata.disaggregation_new
                        (source_id, name, type_id, source_system_id)
                    VALUES (:id, :name, 'CategoryCombo', :src_id)
                    ON CONFLICT (source_system_id, source_id) DO UPDATE
                        SET name    = EXCLUDED.name,
                            type_id = EXCLUDED.type_id
                    RETURNING disaggregation_id
                """), {
                    "id": cc_id, "name": cc_name,
                    "src_id": self.source_system_id,
                })
                if result.scalar():
                    inserted += 1
            s.commit()
            logger.info(f"  Inserted/updated {inserted} CCs")
        except Exception:
            s.rollback()
            logger.exception("sync_category_combos failed")
            raise
        finally:
            s.close()

    # ------------------------------------------------------------------ #
    def sync_de_to_disagg(self):
        logger.info(f"Syncing DE↔CC bridge for source={self.source_system}...")
        mapping = self.extractor.get_data_element_category_combos()
        logger.info(f"  Fetched {len(mapping)} DE→CC mappings")

        s = self.Session()
        try:
            s.execute(
                text("DELETE FROM metadata.data_element_to_disaggregation "
                     "WHERE source_system_id = :src_id"),
                {"src_id": self.source_system_id},
            )

            inserted = 0
            missing_de = 0
            missing_cc = 0

            for de_uid, combos in mapping.items():
                for cc_uid in combos:
                    result = s.execute(text("""
                        INSERT INTO metadata.data_element_to_disaggregation
                            (data_element_id, disaggregation_id, source_system_id)
                        SELECT de.data_element_id, dis.disaggregation_id, :src_id
                        FROM metadata.data_element_new de
                        JOIN metadata.disaggregation_new dis
                          ON dis.source_id = :cc
                         AND dis.source_system_id = :src_id
                        WHERE de.source_id = :de
                          AND de.source_system_id = :src_id
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """), {
                        "de": de_uid, "cc": cc_uid,
                        "src_id": self.source_system_id,
                    })
                    if result.scalar():
                        inserted += 1
                    else:
                        # Diagnose
                        chk_de = s.execute(text(
                            "SELECT 1 FROM metadata.data_element_new "
                            "WHERE source_id=:de AND source_system_id=:src"
                        ), {"de": de_uid, "src": self.source_system_id}).scalar()
                        if not chk_de:
                            missing_de += 1
                            continue
                        chk_cc = s.execute(text(
                            "SELECT 1 FROM metadata.disaggregation_new "
                            "WHERE source_id=:cc AND source_system_id=:src"
                        ), {"cc": cc_uid, "src": self.source_system_id}).scalar()
                        if not chk_cc:
                            missing_cc += 1

            s.commit()
            logger.info(f"  Inserted {inserted} bridge rows")
            if missing_de:
                logger.warning(f"  Missing DEs: {missing_de}")
            if missing_cc:
                logger.warning(f"  Missing CCs: {missing_cc}")
        except Exception:
            s.rollback()
            logger.exception("sync_de_to_disagg failed")
            raise
        finally:
            s.close()

    # ------------------------------------------------------------------ #
    def sync_combo_options(self):
        logger.info(f"Syncing combo_options for source={self.source_system}...")
        s = self.Session()
        try:
            s.execute(
                text("""
                    DELETE FROM metadata.combo_option
                    WHERE disaggregation_id IN (
                        SELECT disaggregation_id FROM metadata.disaggregation_new
                        WHERE source_system_id = :src_id
                    )
                """),
                {"src_id": self.source_system_id},
            )

            rows = s.execute(text("""
                SELECT disaggregation_id, name
                FROM metadata.disaggregation_new
                WHERE name IS NOT NULL
                  AND name != ''
                  AND source_system_id = :src_id
                  AND (type_id IS NULL OR type_id != 'CategoryCombo')
            """), {"src_id": self.source_system_id}).fetchall()

            inserted = 0
            for dis_id, full_name in rows:
                parts = [p.strip() for p in full_name.split(",") if p.strip()]
                for part in parts:
                    s.execute(text("""
                        INSERT INTO metadata.combo_option
                            (disaggregation_id, name, option_code)
                        VALUES (:dis, :name, NULL)
                    """), {"dis": dis_id, "name": part})
                    inserted += 1

            s.commit()
            logger.info(
                f"  Inserted {inserted} combo options from {len(rows)} disaggregations"
            )
        except Exception:
            s.rollback()
            logger.exception("sync_combo_options failed")
            raise
        finally:
            s.close()

    # ------------------------------------------------------------------ #
    def run_all(self):
        self.sync_category_combos()
        self.sync_de_to_disagg()
        self.sync_combo_options()
        self.report()

    def report(self):
        s = self.Session()
        try:
            r = s.execute(text("""
                SELECT
                    (SELECT COUNT(*) FROM metadata.disaggregation_new
                        WHERE source_system_id = :src_id)                      AS total_disagg,
                    (SELECT COUNT(*) FROM metadata.disaggregation_new
                        WHERE source_system_id = :src_id
                          AND type_id = 'CategoryCombo')                        AS ccs,
                    (SELECT COUNT(*) FROM metadata.data_element_to_disaggregation
                        WHERE source_system_id = :src_id)                       AS bridges,
                    (SELECT COUNT(*) FROM metadata.combo_option co
                        JOIN metadata.disaggregation_new d
                          ON d.disaggregation_id = co.disaggregation_id
                        WHERE d.source_system_id = :src_id)                     AS options
            """), {"src_id": self.source_system_id}).fetchone()

            logger.info("=" * 60)
            logger.info(f"Bridge summary for {self.source_system}")
            logger.info("=" * 60)
            logger.info(f"  Disaggregations:    {r[0]:>8}")
            logger.info(f"  Category combos:    {r[1]:>8}")
            logger.info(f"  DE↔CC bridge rows:  {r[2]:>8}")
            logger.info(f"  Combo options:      {r[3]:>8}")
            logger.info("=" * 60)
        finally:
            s.close()


# ====================================================================== #
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["pih", "moh"], default="pih")
    args = parser.parse_args()

    if args.source == "moh":
        b = BridgePopulator(source_system_id=2, source_system="DHIS2_MOH")
    else:
        b = BridgePopulator(source_system_id=1, source_system="DHIS2_PIH")

    b.run_all()
