"""
Database loader for the DHIS2 → PostgreSQL pipeline.

Responsibilities:
  - Create/verify tables
  - Load raw DHIS2 data values into bronze.dhis2_raw (upsert)
  - Sync metadata.* dimension tables from DHIS2
  - Sync silver.dhis2_data from bronze.dhis2_raw (typed, named, deduplicated)
  - Write audit logs to bronze.etl_batch_log

Uses SQLAlchemy 2.x + psycopg2 with PostgreSQL ON CONFLICT upserts.
"""

import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from config import config
from models import (
    Base, DataValue, ExtractionLog,
    ALL_METADATA,
)

logger = logging.getLogger(__name__)


class DBLoader:
    """All database interaction for the DHIS2 ETL pipeline."""

    def __init__(self):
        self.engine = create_engine(
            config.db_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            echo=False,
        )
        self.Session = sessionmaker(bind=self.engine)

    # ================================================================== #
    # DDL
    # ================================================================== #
    def create_tables(self):
        """Create all ORM-managed tables across schemas."""
        for md in ALL_METADATA:
            md.create_all(self.engine)
        logger.info("ORM tables created/verified")

    # ================================================================== #
    # Bronze load
    # ================================================================== #
    def load_data_values(self, dhis_data, de_meta=None, ou_meta=None,
                         batch_size=1000, source_system="DHIS2_PIH",
                         batch_id=None, dataset_id=None):
        """
        Upsert DHIS2 data values into bronze.dhis2_raw.

        Args:
            dhis_data:     Parsed JSON from /api/dataValueSets.
            de_meta:       {dataElementId: {name, code, value_type}}
            ou_meta:       {orgUnitId: {name, code, path, level}}
            batch_size:    Rows per INSERT.
            source_system: Tag for source_system column.
            batch_id:      Optional batch identifier for audit.
            dataset_id:    Dataset UID (if known).

        Returns:
            (received, loaded) tuple of ints.
        """
        de_meta = de_meta or {}
        ou_meta = ou_meta or {}
        values = dhis_data.get("dataValues", [])
        if not values:
            logger.warning("No dataValues to load")
            return 0, 0

        session = self.Session()
        loaded = 0
        try:
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                rows = [
                    self._to_row(v, de_meta, ou_meta,
                                 source_system, batch_id, dataset_id)
                    for v in batch
                ]

                stmt = insert(DataValue).values(rows)
                update_cols = {
                    c.name: stmt.excluded[c.name]
                    for c in DataValue.__table__.columns
                    if c.name not in ("id", "created_at")
                }
                upsert = stmt.on_conflict_do_update(
                    constraint="uq_data_value",
                    set_=update_cols,
                )
                session.execute(upsert)
                loaded += len(rows)

            session.commit()
            logger.info(f"Upserted {loaded} data values into bronze")
            return len(values), loaded

        except Exception:
            session.rollback()
            logger.exception("Failed to load data values")
            raise
        finally:
            session.close()

    # ================================================================== #
    # Row transformation
    # ================================================================== #
    @staticmethod
    def _to_row(v, de_meta, ou_meta, source_system, batch_id, dataset_id):
        """Convert a DHIS2 dataValue dict into a bronze row dict."""
        de = de_meta.get(v.get("dataElement"), {})
        ou = ou_meta.get(v.get("orgUnit"), {})

        # ---- value handling ----
        raw_value = v.get("value")
        if raw_value is None or str(raw_value).strip() in ("", "nil"):
            value_str = None
        else:
            value_str = str(raw_value)

        value_numeric = None
        if value_str is not None:
            try:
                value_numeric = Decimal(value_str)
            except (InvalidOperation, ValueError):
                value_numeric = None

        # ---- timestamp ----
        last_updated = v.get("lastUpdated")
        if last_updated:
            try:
                last_updated = datetime.fromisoformat(
                    last_updated.replace("Z", "+00:00")
                )
            except (ValueError, AttributeError):
                last_updated = None

        # ---- followup ----
        followup_raw = v.get("followup")
        followup = str(followup_raw)[:10] if followup_raw is not None else None

        return {
            # Core DHIS2 fields
            "data_element":             v.get("dataElement"),
            "data_element_name":        de.get("name"),
            "data_element_code":        de.get("code"),
            "data_element_value_type":  de.get("value_type"),
            "period":                   v.get("period"),
            "org_unit":                 v.get("orgUnit"),
            "org_unit_name":            ou.get("name"),
            "category_option_combo":    v.get("categoryOptionCombo"),
            "attribute_option_combo":   v.get("attributeOptionCombo"),
            "value":                    value_str,
            "value_numeric":            value_numeric,
            "stored_by":                v.get("storedBy"),
            "last_updated":             last_updated,
            "comment":                  v.get("comment"),
            "followup":                 followup,
            # Diagram-aligned columns
            "source_system":            source_system,
            "dataset_id":               dataset_id,
            "dataset_name":             None,
            "de_name":                  de.get("name"),
            "de_description":           None,
            "disaggregation":           v.get("categoryOptionCombo"),
            "batch_id":                 batch_id,
            "raw_data":                 None,
        }

    # ================================================================== #
    # Metadata sync (DHIS2 → metadata.*)
    # ================================================================== #
    def sync_metadata(self, source_system_id=1):
        """
        Fetch all DHIS2 metadata and upsert into metadata.* tables.

        Args:
            source_system_id: FK into metadata.source_system.
                              1 = DHIS2 PIH, 2 = DHIS2 MOH.
        """
        from dhis2_extractor import DHIS2Extractor

        ex = DHIS2Extractor()
        logger.info("Fetching metadata from DHIS2...")

        datasets      = ex.get_datasets_full()
        data_elements = ex.get_data_elements_full()
        org_units     = ex.get_org_units_full()
        cocs          = ex.get_category_option_combos()

        logger.info(
            f"  {len(datasets)} datasets | "
            f"{len(data_elements)} data elements | "
            f"{len(org_units)} org units | "
            f"{len(cocs)} category option combos"	
        )

        session = self.Session()
        try:
            # --- datasets ---
            for ds in datasets:
                session.execute(text("""
                    INSERT INTO metadata.dataset
                        (dataset_id, dataset_name, dataset_description,
                         period_type, source_system_id)
                    VALUES (:id, :name, :desc, :ptype, :src)
                    ON CONFLICT (dataset_id) DO UPDATE
                        SET dataset_name        = EXCLUDED.dataset_name,
                            dataset_description = EXCLUDED.dataset_description,
                            period_type         = EXCLUDED.period_type,
                            synced_at           = now()
                """), {
                    "id":    ds["id"],
                    "name":  ds.get("displayName") or ds.get("name"),
                    "desc":  ds.get("description"),
                    "ptype": ds.get("periodType"),
                    "src":   source_system_id,
                })

            # --- data elements ---
            for de in data_elements:
                session.execute(text("""
                    INSERT INTO metadata.data_element
                        (de_id, data_element_name, data_element_description,
                         data_element_type, source_system_id)
                    VALUES (:id, :name, :desc, :type, :src)
                    ON CONFLICT (de_id) DO UPDATE
                        SET data_element_name        = EXCLUDED.data_element_name,
                            data_element_description = EXCLUDED.data_element_description,
                            data_element_type        = EXCLUDED.data_element_type,
                            synced_at                = now()
                """), {
                    "id":   de["id"],
                    "name": de.get("displayName") or de.get("name"),
                    "desc": de.get("description"),
                    "type": de.get("valueType"),
                    "src":  source_system_id,
                })

            # --- org units ---
            for ou in org_units:
                session.execute(text("""
                    INSERT INTO metadata.orgunit
                        (orgunit_id, orgunit_name, orgunit_level,
                         parent_id, path, source_system_id)
                    VALUES (:id, :name, :lvl, :parent, :path, :src)
                    ON CONFLICT (orgunit_id) DO UPDATE
                        SET orgunit_name  = EXCLUDED.orgunit_name,
                            orgunit_level = EXCLUDED.orgunit_level,
                            parent_id     = EXCLUDED.parent_id,
                            path          = EXCLUDED.path,
                            synced_at     = now()
                """), {
                    "id":     ou["id"],
                    "name":   ou.get("displayName") or ou.get("name"),
                    "lvl":    ou.get("level"),
                    "parent": (ou.get("parent") or {}).get("id"),
                    "path":   ou.get("path"),
                    "src":    source_system_id,
                })

            # --- disaggregations ---
            for coc in cocs:
                session.execute(text("""
                    INSERT INTO metadata.disaggregation
                        (disaggregation_id, disaggregation_name,
                         disaggregation_type, option_combo_id, source_system_id)
                    VALUES (:id, :name, :type, :combo, :src)
                    ON CONFLICT (disaggregation_id) DO UPDATE
                        SET disaggregation_name = EXCLUDED.disaggregation_name,
                            synced_at           = now()
                """), {
                    "id":    coc["id"],
                    "name":  coc.get("displayName") or coc.get("name"),
                    "type":  "CategoryOptionCombo",
                    "combo": coc["id"],
                    "src":   source_system_id,
                })

            session.commit()
            logger.info("Metadata sync complete")
        except Exception:
            session.rollback()
            logger.exception("Metadata sync failed")
            raise
        finally:
            session.close()

    # ================================================================== #
    # Silver sync (bronze.dhis2_raw → silver.dhis2_data)
    # ================================================================== #
    def sync_silver(self, source_system="DHIS2_PIH"):
        """
        Populate silver.dhis2_data from bronze.dhis2_raw.

        Uses DISTINCT ON to deduplicate rows that resolve to the same
        (source, dataset, de, disaggregation, period, orgunit) key.
        Keeps the row with the most recent last_updated timestamp.

        No summing — preserves the actual values from DHIS2.
        """
        session = self.Session()
        try:
            logger.info(f"Syncing silver from bronze ({source_system})...")

            # Ensure target table exists
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS silver.dhis2_data (
                    id              BIGSERIAL PRIMARY KEY,
                    source_system   VARCHAR(20),
                    dataset_id      VARCHAR(50),
                    dataset_name    VARCHAR(500),
                    de_id           VARCHAR(50),
                    de_name         VARCHAR(500),
                    de_description  TEXT,
                    disaggregation  VARCHAR(500),
                    period          VARCHAR(20),
                    orgunit_id      VARCHAR(50),
                    orgunit_name    VARCHAR(500),
                    value           NUMERIC(20,6),
                    last_updated    TIMESTAMPTZ,
                    loaded_at       TIMESTAMPTZ DEFAULT now(),
                    batch_id        VARCHAR(50)
                )
            """))

            session.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_silver_row
                ON silver.dhis2_data
                (source_system, dataset_id, de_id, disaggregation,
                 period, orgunit_id)
            """))

            # Clear existing data for this source before reload
            session.execute(
                text("DELETE FROM silver.dhis2_data WHERE source_system = :src"),
                {"src": source_system}
            )

            logger.info("Extracting from bronze (deduplicating via DISTINCT ON)...")

            result = session.execute(text("""
                INSERT INTO silver.dhis2_data
                    (source_system, dataset_id, dataset_name,
                     de_id, de_name, de_description,
                     disaggregation, period,
                     orgunit_id, orgunit_name,
                     value, last_updated, batch_id)
                SELECT DISTINCT ON (
                    b.source_system,
                    b.dataset_id,
                    b.data_element,
                    COALESCE(dis.disaggregation_name, b.disaggregation, 'Total'),
                    b.period,
                    b.org_unit
                )
                    b.source_system,
                    b.dataset_id,
                    ds.dataset_name,
                    b.data_element                                           AS de_id,
                    COALESCE(de.data_element_name, b.data_element_name)      AS de_name,
                    de.data_element_description,
                    COALESCE(dis.disaggregation_name, b.disaggregation, 'Total') AS disaggregation,
                    b.period,
                    b.org_unit                                               AS orgunit_id,
                    COALESCE(mo.orgunit_name, b.org_unit_name)               AS orgunit_name,
                    b.value_numeric                                          AS value,
                    b.last_updated,
                    b.batch_id
                FROM bronze.dhis2_raw b
                LEFT JOIN metadata.data_element     de  ON de.de_id            = b.data_element
                LEFT JOIN metadata.dataset          ds  ON ds.dataset_id       = COALESCE(b.dataset_id, de.dataset_id)
                LEFT JOIN metadata.disaggregation   dis ON dis.disaggregation_id = b.disaggregation
                LEFT JOIN metadata.orgunit          mo  ON mo.orgunit_id       = b.org_unit
                WHERE b.value_numeric IS NOT NULL
                  AND b.source_system = :src
                ORDER BY
                    b.source_system,
                    b.dataset_id,
                    b.data_element,
                    COALESCE(dis.disaggregation_name, b.disaggregation, 'Total'),
                    b.period,
                    b.org_unit,
                    b.last_updated DESC NULLS LAST
            """), {"src": source_system})

            session.commit()
            logger.info(f"Silver sync complete: {result.rowcount} rows inserted")
            return result.rowcount

        except Exception:
            session.rollback()
            logger.exception("Silver sync failed")
            raise
        finally:
            session.close()

    # ================================================================== #
    # Audit log
    # ================================================================== #
    def log_extraction(self, dataset_id, period, org_units, json_file,
                       received, loaded, status, error=None):
        """Write an entry to bronze.etl_batch_log."""
        session = self.Session()
        try:
            log = ExtractionLog(
                dataset_id=dataset_id,
                period=str(period),
                org_units=",".join(org_units) if isinstance(org_units, list)
                          else str(org_units),
                json_file=str(json_file) if json_file else "",
                records_received=received,
                records_loaded=loaded,
                status=status,
                error_message=error,
                finished_at=datetime.utcnow(),
            )
            session.add(log)
            session.commit()
            logger.info(f"Logged extraction: {status} ({loaded}/{received})")
        except Exception:
            session.rollback()
            logger.exception("Failed to write extraction log")
        finally:
            session.close()

    # ================================================================== #
    # Convenience queries
    # ================================================================== #
    def count_rows(self, schema="bronze", table="dhis2_raw"):
        with self.engine.connect() as conn:
            return conn.execute(
                text(f"SELECT COUNT(*) FROM {schema}.{table}")
            ).scalar()

    def summarize_by_value_type(self):
        sql = text("""
            SELECT data_element_value_type,
                   COUNT(*) AS total,
                   COUNT(value_numeric) AS numeric_count
            FROM bronze.dhis2_raw
            GROUP BY data_element_value_type
            ORDER BY total DESC
        """)
        with self.engine.connect() as conn:
            return [dict(r._mapping) for r in conn.execute(sql)]

    def summarize_silver_by_source(self):
        sql = text("""
            SELECT source_system,
                   COUNT(*) AS rows,
                   COUNT(DISTINCT de_id) AS distinct_de,
                   COUNT(DISTINCT orgunit_id) AS distinct_ou,
                   SUM(value) AS total_value
            FROM silver.dhis2_data
            GROUP BY source_system
            ORDER BY rows DESC
        """)
        with self.engine.connect() as conn:
            return [dict(r._mapping) for r in conn.execute(sql)]
