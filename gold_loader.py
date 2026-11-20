"""
Gold Layer Loader (Star Schema).
Builds dimensions and the central fact table from silver.dhis2_data.
"""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config import config

logger = logging.getLogger(__name__)


class GoldLoader:
    """Builds and refreshes the Gold star schema from Silver."""

    def __init__(self):
        self.engine = create_engine(
            config.db_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        self.Session = sessionmaker(bind=self.engine)

    # ================================================================== #
    # DDL
    # ================================================================== #
    def create_gold_schema(self):
        """Create dimension tables and the fact table (idempotent)."""
        logger.info("Creating Gold star schema...")
        session = self.Session()
        try:
            # 1. dim_source_system
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS gold.dim_source_system (
                    source_system_id    SERIAL PRIMARY KEY,
                    source_system_code  VARCHAR(20) UNIQUE NOT NULL,
                    source_system_name  VARCHAR(100),
                    source_system_desc  TEXT
                );
            """))

            # 2. dim_date
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS gold.dim_date (
                    date_id    INTEGER PRIMARY KEY,   -- YYYYMMDD
                    full_date  DATE,
                    year       INTEGER,
                    quarter    INTEGER,
                    month      INTEGER,
                    week       INTEGER,
                    day        INTEGER,
                    period_str VARCHAR(10)            -- matches silver.period (monthly only)
                );
            """))

            # 3. dim_dataset
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS gold.dim_dataset (
                    dataset_id          SERIAL PRIMARY KEY,
                    source_dataset_id   VARCHAR(50) UNIQUE NOT NULL,
                    dataset_name        VARCHAR(500),
                    dataset_description TEXT,
                    program_area        VARCHAR(100)
                );
            """))

            # 4. dim_data_element
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS gold.dim_data_element (
                    data_element_id           SERIAL PRIMARY KEY,
                    source_de_id              VARCHAR(50) UNIQUE NOT NULL,
                    data_element_name         VARCHAR(500),
                    data_element_description  TEXT,
                    data_element_type         VARCHAR(50)
                );
            """))

            # 5. dim_disaggregation (source_disagg_id widened to 500)
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS gold.dim_disaggregation (
                    disaggregation_id   SERIAL PRIMARY KEY,
                    source_disagg_id    VARCHAR(500),
                    disaggregation_name VARCHAR(500),
                    disaggregation_type VARCHAR(50),
                    UNIQUE (source_disagg_id, disaggregation_name)
                );
            """))

            # 6. dim_orgunit
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS gold.dim_orgunit (
                    orgunit_id        SERIAL PRIMARY KEY,
                    source_orgunit_id VARCHAR(50) UNIQUE NOT NULL,
                    orgunit_name      VARCHAR(500),
                    orgunit_level     INTEGER,
                    parent_id         VARCHAR(50),
                    district          VARCHAR(100),
                    region            VARCHAR(100),
                    facility_type     VARCHAR(50)
                );
            """))

            # 7. fact_health_data
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS gold.fact_health_data (
                    id                BIGSERIAL PRIMARY KEY,
                    date_id           INTEGER REFERENCES gold.dim_date(date_id),
                    orgunit_id        INTEGER REFERENCES gold.dim_orgunit(orgunit_id),
                    data_element_id   INTEGER REFERENCES gold.dim_data_element(data_element_id),
                    source_system_id  INTEGER REFERENCES gold.dim_source_system(source_system_id),
                    disaggregation_id INTEGER REFERENCES gold.dim_disaggregation(disaggregation_id),
                    dataset_id        INTEGER REFERENCES gold.dim_dataset(dataset_id),
                    value             NUMERIC(20,6),
                    record_count      INTEGER DEFAULT 1,
                    last_updated      TIMESTAMPTZ,
                    loaded_at         TIMESTAMPTZ DEFAULT now()
                );
            """))

            # Indexes for Power BI
            session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_fact_date ON gold.fact_health_data (date_id);"
            ))
            session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_fact_org  ON gold.fact_health_data (orgunit_id);"
            ))
            session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_fact_de   ON gold.fact_health_data (data_element_id);"
            ))
            session.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_fact_src  ON gold.fact_health_data (source_system_id);"
            ))

            session.commit()
            logger.info("Gold schema created.")
        except Exception:
            session.rollback()
            logger.exception("Failed to create Gold schema")
            raise
        finally:
            session.close()

    # ================================================================== #
    # Dimensions
    # ================================================================== #
    def sync_dimensions(self):
        """Populate all dimension tables from metadata.* and silver."""
        logger.info("Syncing Gold dimensions...")
        session = self.Session()
        try:
            # 1. Source systems — unique code derived from source_name
            session.execute(text("""
                INSERT INTO gold.dim_source_system
                    (source_system_code, source_system_name, source_system_desc)
                SELECT
                    REGEXP_REPLACE(LOWER(source_name), '[^a-z0-9]+', '_', 'g'),
                    source_name,
                    source_type || COALESCE(' — ' || description, '')
                FROM metadata.source_system
                WHERE active = TRUE
                ON CONFLICT (source_system_code) DO UPDATE
                    SET source_system_name = EXCLUDED.source_system_name,
                        source_system_desc = EXCLUDED.source_system_desc;
            """))

            # 2. Datasets
            session.execute(text("""
                INSERT INTO gold.dim_dataset
                    (source_dataset_id, dataset_name, dataset_description, program_area)
                SELECT dataset_id, dataset_name, dataset_description, program_area
                FROM metadata.dataset
                ON CONFLICT (source_dataset_id) DO UPDATE
                    SET dataset_name        = EXCLUDED.dataset_name,
                        dataset_description = EXCLUDED.dataset_description,
                        program_area        = EXCLUDED.program_area;
            """))

            # 3. Data elements
            session.execute(text("""
                INSERT INTO gold.dim_data_element
                    (source_de_id, data_element_name,
                     data_element_description, data_element_type)
                SELECT de_id, data_element_name,
                       data_element_description, data_element_type
                FROM metadata.data_element
                ON CONFLICT (source_de_id) DO UPDATE
                    SET data_element_name        = EXCLUDED.data_element_name,
                        data_element_description = EXCLUDED.data_element_description,
                        data_element_type        = EXCLUDED.data_element_type;
            """))

            # 4. Disaggregations (from silver — source of truth for what's used)
            session.execute(text("""
                INSERT INTO gold.dim_disaggregation
                    (source_disagg_id, disaggregation_name, disaggregation_type)
                SELECT DISTINCT disaggregation, disaggregation, 'Standard'
                FROM silver.dhis2_data
                WHERE disaggregation IS NOT NULL
                ON CONFLICT (source_disagg_id, disaggregation_name) DO NOTHING;
            """))

            # 5. Org units
            session.execute(text("""
                INSERT INTO gold.dim_orgunit
                    (source_orgunit_id, orgunit_name, orgunit_level,
                     parent_id, district, region)
                SELECT orgunit_id, orgunit_name, orgunit_level,
                       parent_id, district, region
                FROM metadata.orgunit
                ON CONFLICT (source_orgunit_id) DO UPDATE
                    SET orgunit_name  = EXCLUDED.orgunit_name,
                        orgunit_level = EXCLUDED.orgunit_level,
                        parent_id     = EXCLUDED.parent_id,
                        district      = EXCLUDED.district,
                        region        = EXCLUDED.region;
            """))

            # 6. Date (2015–2030, one row per day)
            session.execute(text("""
                INSERT INTO gold.dim_date
                    (date_id, full_date, year, quarter, month, week, day, period_str)
                SELECT
                    CAST(TO_CHAR(d, 'YYYYMMDD') AS INTEGER),
                    d,
                    EXTRACT(YEAR    FROM d)::int,
                    EXTRACT(QUARTER FROM d)::int,
                    EXTRACT(MONTH   FROM d)::int,
                    EXTRACT(WEEK    FROM d)::int,
                    EXTRACT(DAY     FROM d)::int,
                    TO_CHAR(d, 'YYYYMM')
                FROM generate_series(
                    '2015-01-01'::date, '2030-12-31'::date, '1 day'::interval
                ) d
                ON CONFLICT (date_id) DO NOTHING;
            """))

            session.commit()
            logger.info("Gold dimensions synced.")
        except Exception:
            session.rollback()
            logger.exception("Failed to sync Gold dimensions")
            raise
        finally:
            session.close()

    # ================================================================== #
    # Fact
    # ================================================================== #
    def sync_fact(self):
        """
        Rebuild gold.fact_health_data from silver.dhis2_data.
        Handles monthly (YYYYMM), quarterly (YYYYQn), and yearly (YYYY) periods.
        """
        logger.info("Syncing Gold fact table (this may take a minute)...")
        session = self.Session()
        try:
            session.execute(text("TRUNCATE gold.fact_health_data;"))

            result = session.execute(text("""
                INSERT INTO gold.fact_health_data (
                    date_id, orgunit_id, data_element_id, source_system_id,
                    disaggregation_id, dataset_id, value, last_updated
                )
                SELECT
                    CAST(
                        CASE
                            -- Monthly: YYYYMM -> YYYYMM01
                            WHEN LENGTH(s.period) = 6 AND s.period ~ '^[0-9]+$'
                                THEN s.period || '01'
                            -- Quarterly: YYYYQn -> YYYY<Q*3>01
                            WHEN s.period ~ '^[0-9]{4}Q[1-4]$'
                                THEN SUBSTRING(s.period, 1, 4)
                                     || LPAD(
                                            (CAST(SUBSTRING(s.period, 6, 1) AS INTEGER) * 3)::text,
                                            2, '0'
                                        )
                                     || '01'
                            -- Yearly: YYYY -> YYYY0101
                            WHEN LENGTH(s.period) = 4 AND s.period ~ '^[0-9]+$'
                                THEN s.period || '0101'
                            ELSE NULL
                        END AS INTEGER
                    ) AS date_id,
                    ou.orgunit_id,
                    de.data_element_id,
                    ss.source_system_id,
                    dis.disaggregation_id,
                    ds.dataset_id,
                    s.value,
                    s.last_updated
                FROM silver.dhis2_data s
                LEFT JOIN gold.dim_orgunit         ou  ON ou.source_orgunit_id  = s.orgunit_id
                LEFT JOIN gold.dim_data_element    de  ON de.source_de_id       = s.de_id
                LEFT JOIN gold.dim_source_system   ss  ON ss.source_system_code =
                                                         LOWER(REPLACE(s.source_system, ' ', '_'))
                LEFT JOIN gold.dim_disaggregation  dis ON dis.source_disagg_id  = s.disaggregation
                LEFT JOIN gold.dim_dataset         ds  ON ds.source_dataset_id  = s.dataset_id
                WHERE s.period IS NOT NULL
                  AND (
                       (LENGTH(s.period) = 6 AND s.period ~ '^[0-9]+$')
                       OR (s.period ~ '^[0-9]{4}Q[1-4]$')
                       OR (LENGTH(s.period) = 4 AND s.period ~ '^[0-9]+$')
                  )
            """))

            session.commit()
            logger.info(f"Gold fact sync complete: {result.rowcount} rows inserted.")
            return result.rowcount

        except Exception:
            session.rollback()
            logger.exception("Failed to sync Gold fact")
            raise
        finally:
            session.close()

    # ================================================================== #
    # Orchestration
    # ================================================================== #
    def refresh_all(self):
        """Full Gold rebuild: schema + dims + fact."""
        self.create_gold_schema()
        self.sync_dimensions()
        self.sync_fact()
