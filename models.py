"""
SQLAlchemy models for the DHIS2 → PostgreSQL pipeline.

Schemas:
  - bronze:   raw DHIS2 data + audit log
  - metadata: dimension catalogs (data elements, org units, disaggregations, datasets)
  - silver:   (managed via raw SQL in db_loader.py, not ORM)
  - gold:     (managed via raw SQL in db_loader.py, not ORM)

Note: silver.* and gold.* are created via raw DDL because they involve
      complex transformations and materialized views. Only bronze and
      metadata are ORM-managed here.
"""

from sqlalchemy import (
    Column, Integer, BigInteger, String, Numeric, DateTime, Text,
    Boolean, Index, UniqueConstraint, MetaData, ForeignKey
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func


# ===================================================================== #
# BRONZE SCHEMA — raw DHIS2 data
# ===================================================================== #
BRONZE_SCHEMA = "bronze"
bronze_metadata = MetaData(schema=BRONZE_SCHEMA)
BronzeBase = declarative_base(metadata=bronze_metadata)


class DataValue(BronzeBase):
    """
    Raw DHIS2 data values.
    One row = one (dataElement, period, orgUnit, categoryOptionCombo) tuple.
    """
    __tablename__ = "dhis2_raw"
    __table_args__ = (
        UniqueConstraint(
            "data_element", "period", "org_unit",
            "category_option_combo", "attribute_option_combo",
            name="uq_data_value",
        ),
        Index("idx_de_period_ou", "data_element", "period", "org_unit"),
        {"schema": BRONZE_SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ---- Core DHIS2 fields (UIDs + values) ----
    data_element            = Column(String(50), nullable=False, index=True)
    data_element_name       = Column(String(500))
    data_element_code       = Column(String(100), index=True)
    data_element_value_type = Column(String(50))
    period                  = Column(String(20), nullable=False, index=True)
    org_unit                = Column(String(50), nullable=False, index=True)
    org_unit_name           = Column(String(500))
    category_option_combo   = Column(String(50))
    attribute_option_combo  = Column(String(50))
    value                   = Column(Text)
    value_numeric           = Column(Numeric(20, 6), nullable=True)
    stored_by               = Column(String(100))
    last_updated            = Column(DateTime(timezone=True))
    comment                 = Column(Text)
    followup                = Column(String(10))
    created_at              = Column(DateTime(timezone=True), server_default=func.now())

    # ---- Diagram-aligned columns ----
    source_system           = Column(String(20), default="DHIS2_PIH", index=True)
    dataset_id              = Column(String(50), index=True)
    dataset_name            = Column(String(500))
    de_name                 = Column(String(500))
    de_description          = Column(Text)
    disaggregation          = Column(String(500))
    batch_id                = Column(String(50), index=True)
    raw_data                = Column(JSONB)
    loaded_at               = Column(DateTime(timezone=True), server_default=func.now())


class ExtractionLog(BronzeBase):
    """
    Audit log of every extraction run (renamed from extraction_log).
    Also used to record per-dataset status during full-extraction runs.
    """
    __tablename__ = "etl_batch_log"
    __table_args__ = {"schema": BRONZE_SCHEMA}

    id                = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id        = Column(String(50), index=True)
    period            = Column(String(50), index=True)
    org_units         = Column(Text)
    json_file         = Column(String(500))
    records_received  = Column(Integer)
    records_loaded    = Column(Integer)
    status            = Column(String(20))
    error_message     = Column(Text)
    started_at        = Column(DateTime(timezone=True), server_default=func.now())
    finished_at       = Column(DateTime(timezone=True))


# ===================================================================== #
# METADATA SCHEMA — dimension catalogs
# ===================================================================== #
METADATA_SCHEMA = "metadata"
metadata_metadata = MetaData(schema=METADATA_SCHEMA)
MetadataBase = declarative_base(metadata=metadata_metadata)


class SourceSystem(MetadataBase):
    """
    Registry of data sources (DHIS2 PIH, DHIS2 MOH, CommCare, OpenMRS, LIS...).
    Already seeded with 5 rows in your DB.
    """
    __tablename__ = "source_system"
    __table_args__ = {"schema": METADATA_SCHEMA}

    source_system_id = Column(BigInteger, primary_key=True, autoincrement=True)
    source_name      = Column(String(100), nullable=False)
    source_type      = Column(String(50))
    description      = Column(Text)
    base_url         = Column(Text)
    active           = Column(Boolean, default=True)
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), server_default=func.now(),
                              onupdate=func.now())


class Dataset(MetadataBase):
    """DHIS2 datasets (used to resolve dataset_name in silver)."""
    __tablename__ = "dataset"
    __table_args__ = {"schema": METADATA_SCHEMA}

    dataset_id           = Column(String(50), primary_key=True)
    dataset_name         = Column(String(500))
    dataset_description  = Column(Text)
    period_type          = Column(String(50))
    program_area         = Column(String(100))
    source_system_id     = Column(
        BigInteger,
        ForeignKey(f"{METADATA_SCHEMA}.source_system.source_system_id"),
    )
    active_flag          = Column(Boolean, default=True)
    synced_at            = Column(DateTime(timezone=True), server_default=func.now())


class DataElement(MetadataBase):
    """DHIS2 data elements (used to resolve de_name and de_description)."""
    __tablename__ = "data_element"
    __table_args__ = {"schema": METADATA_SCHEMA}

    de_id                     = Column(String(50), primary_key=True)
    data_element_name         = Column(String(500))
    data_element_description  = Column(Text)
    data_element_type         = Column(String(50))
    dataset_id                = Column(
        String(50),
        ForeignKey(f"{METADATA_SCHEMA}.dataset.dataset_id"),
    )
    source_system_id          = Column(
        BigInteger,
        ForeignKey(f"{METADATA_SCHEMA}.source_system.source_system_id"),
    )
    synced_at                 = Column(DateTime(timezone=True), server_default=func.now())


class OrgUnit(MetadataBase):
    """DHIS2 organisation units with hierarchy."""
    __tablename__ = "orgunit"
    __table_args__ = (
        Index("idx_meta_ou_parent", "parent_id"),
        Index("idx_meta_ou_level",  "orgunit_level"),
        {"schema": METADATA_SCHEMA},
    )

    orgunit_id       = Column(String(50), primary_key=True)
    orgunit_name     = Column(String(500))
    orgunit_level    = Column(Integer)
    parent_id        = Column(String(50))
    facility_type    = Column(String(50))
    district         = Column(String(100))
    region           = Column(String(100))
    path             = Column(Text)
    source_system_id = Column(
        BigInteger,
        ForeignKey(f"{METADATA_SCHEMA}.source_system.source_system_id"),
    )
    synced_at        = Column(DateTime(timezone=True), server_default=func.now())


class Disaggregation(MetadataBase):
    """
    Category option combos.
    Resolves DHIS2 disaggregation UIDs to human-readable names like
    'Male, 15-19'.
    """
    __tablename__ = "disaggregation"
    __table_args__ = {"schema": METADATA_SCHEMA}

    disaggregation_id    = Column(String(50), primary_key=True)
    disaggregation_name  = Column(String(500))
    disaggregation_type  = Column(String(50))
    option_combo_id      = Column(String(50))
    source_system_id     = Column(
        BigInteger,
        ForeignKey(f"{METADATA_SCHEMA}.source_system.source_system_id"),
    )
    synced_at            = Column(DateTime(timezone=True), server_default=func.now())


# ===================================================================== #
# ETL SCHEMA — pipeline control & logging
# ===================================================================== #
ETL_SCHEMA = "etl"
etl_metadata = MetaData(schema=ETL_SCHEMA)
EtlBase = declarative_base(metadata=etl_metadata)


class PipelineControl(EtlBase):
    """Tracks last successful run per pipeline."""
    __tablename__ = "pipeline_control"
    __table_args__ = {"schema": ETL_SCHEMA}

    pipeline_name          = Column(String(100), primary_key=True)
    source_system          = Column(String(50))
    last_successful_run    = Column(DateTime(timezone=True))
    last_extracted_period  = Column(String(20))
    last_run_status        = Column(String(20))
    updated_at             = Column(DateTime(timezone=True), server_default=func.now(),
                                    onupdate=func.now())


class PipelineLog(EtlBase):
    """Per-run pipeline log."""
    __tablename__ = "pipeline_log"
    __table_args__ = {"schema": ETL_SCHEMA}

    pipeline_log_id   = Column(BigInteger, primary_key=True, autoincrement=True)
    pipeline_name     = Column(String(100))
    source_system     = Column(String(50))
    source_endpoint   = Column(Text)
    start_time        = Column(DateTime(timezone=True))
    end_time          = Column(DateTime(timezone=True))
    records_extracted = Column(BigInteger)
    records_loaded    = Column(BigInteger)
    status            = Column(String(20))
    error_message     = Column(Text)
    created_at        = Column(DateTime(timezone=True), server_default=func.now())


# ===================================================================== #
# Backward-compatibility aliases
# ===================================================================== #
# Some parts of the codebase still import `Base` from models.
# Point it to BronzeBase so `Base.metadata.create_all()` continues to work.
Base = BronzeBase


# Convenience list of all metadata objects for create_all()
ALL_METADATA = [
    bronze_metadata,
    metadata_metadata,
    etl_metadata,
]
