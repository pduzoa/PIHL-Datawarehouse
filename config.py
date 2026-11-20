"""
Configuration loader.

Reads all settings from .env and exposes them as a `config` object.
Also provides helper properties (db_url, has_moh) used across the pipeline.
"""

import os

from dotenv import load_dotenv

# Load .env from the current working directory
load_dotenv()


class Config:
    # ================================================================== #
    # DHIS2 PIH (primary source)
    # ================================================================== #
    DHIS2_URL      = os.getenv("DHIS2_URL", "")
    DHIS2_USERNAME = os.getenv("DHIS2_USERNAME", "")
    DHIS2_PASSWORD = os.getenv("DHIS2_PASSWORD", "")

    # ================================================================== #
    # DHIS2 MOH (secondary source)
    # ================================================================== #
    MOH_DHIS2_URL      = os.getenv("MOH_DHIS2_URL", "")
    MOH_DHIS2_USERNAME = os.getenv("MOH_DHIS2_USERNAME", "")
    MOH_DHIS2_PASSWORD = os.getenv("MOH_DHIS2_PASSWORD", "")

    # ================================================================== #
    # PostgreSQL
    # ================================================================== #
    DB_HOST     = os.getenv("DB_HOST", "localhost")
    DB_PORT     = os.getenv("DB_PORT", "5432")
    DB_NAME     = os.getenv("DB_NAME", "dhis2_data")
    DB_USER     = os.getenv("DB_USER", "")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")

    # ================================================================== #
    # Filesystem
    # ================================================================== #
    JSON_TEMP_DIR    = os.getenv("JSON_TEMP_DIR", "json_temp")
    JSON_ARCHIVE_DIR = os.getenv("JSON_ARCHIVE_DIR", "json_archive")

    # ================================================================== #
    # Derived properties
    # ================================================================== #
    @property
    def db_url(self):
        """SQLAlchemy connection string."""
        return (
            f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )

    @property
    def has_pih(self):
        """True if PIH credentials are configured."""
        return bool(self.DHIS2_URL and self.DHIS2_USERNAME)

    @property
    def has_moh(self):
        """True if MOH credentials are configured."""
        return bool(
            self.MOH_DHIS2_URL
            and self.MOH_DHIS2_USERNAME
            and self.MOH_DHIS2_PASSWORD
        )

    # ================================================================== #
    # Diagnostics
    # ================================================================== #
    def summary(self):
        """Return a redacted summary of the configuration (safe to log)."""
        def mask(val):
            if not val:
                return "(missing)"
            return f"{'*' * 6}{val[-2:]}" if len(val) > 2 else "(set)"

        return {
            "PIH": {
                "url":      self.DHIS2_URL or "(missing)",
                "username": self.DHIS2_USERNAME or "(missing)",
                "password": mask(self.DHIS2_PASSWORD),
            },
            "MOH": {
                "url":      self.MOH_DHIS2_URL or "(missing)",
                "username": self.MOH_DHIS2_USERNAME or "(missing)",
                "password": mask(self.MOH_DHIS2_PASSWORD),
            },
            "DB": {
                "host":     self.DB_HOST,
                "port":     self.DB_PORT,
                "name":     self.DB_NAME,
                "user":     self.DB_USER,
                "password": mask(self.DB_PASSWORD),
            },
            "Filesystem": {
                "json_temp":    self.JSON_TEMP_DIR,
                "json_archive": self.JSON_ARCHIVE_DIR,
            },
            "Status": {
                "has_pih": self.has_pih,
                "has_moh": self.has_moh,
            },
        }


# Singleton — imported everywhere as `from config import config`
config = Config()


# ====================================================================== #
# Self-check when run directly
# ====================================================================== #
if __name__ == "__main__":
    import json
    print(json.dumps(config.summary(), indent=2))
