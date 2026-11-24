"""
DHIS2 API client.

Targets DHIS2 2.30 (unversioned /api/ endpoints).
Handles retries, authentication, empty response bodies,
and metadata / data value fetching.

Supports multi-instance via `url`/`username`/`password` arguments:
    ex = DHIS2Extractor()                              # PIH (default)
    ex = DHIS2Extractor(url=MOH_URL, username=..., ...)  # MOH
"""

import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential

from config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


class DHIS2Extractor:
    """Client for a DHIS2 instance (PIH or MOH)."""

    def __init__(self, url=None, username=None, password=None):
        """
        Args:
            url:      DHIS2 base URL. Defaults to config.DHIS2_URL.
            username: DHIS2 username. Defaults to config.DHIS2_USERNAME.
            password: DHIS2 password. Defaults to config.DHIS2_PASSWORD.
        """
        self.base_url = (url or config.DHIS2_URL).rstrip("/")
        # DHIS2 2.30 does not use versioned endpoints for metadata
        self.api_url = f"{self.base_url}/api"

        self._username = username or config.DHIS2_USERNAME
        self._password = password or config.DHIS2_PASSWORD

        self.session = self._build_session()

    # ------------------------------------------------------------------ #
    # Session & HTTP plumbing
    # ------------------------------------------------------------------ #
    def _build_session(self):
        """Create a requests.Session with auth + retry behavior."""
        session = requests.Session()
        session.auth = (self._username, self._password)
        session.headers.update({"Accept": "application/json"})

        retries = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        session.mount("http://", HTTPAdapter(max_retries=retries))
        return session

    # ------------------------------------------------------------------ #
    # Helper: safe JSON parsing
    # ------------------------------------------------------------------ #
    @staticmethod
    def _safe_json(resp, context=""):
        """
        Parse JSON safely, handling empty response bodies.
        
        DHIS2 (especially MOH) sometimes returns HTTP 200 with empty body
        when a dataset has no data. Treat that as {"dataValues": []}.
        """
        if not resp.text or resp.text.strip() == "":
            logger.debug(f"Empty response body {context} — treating as empty result")
            return {"dataValues": []}
        try:
            return resp.json()
        except ValueError as e:
            logger.warning(f"Invalid JSON {context}: {e}. Body starts with: {resp.text[:100]!r}")
            return {"dataValues": []}

    # ------------------------------------------------------------------ #
    # Data values (the fact extraction)
    # ------------------------------------------------------------------ #
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
    def get_data_values(self, data_set_id, org_unit_ids, period):
        """
        Fetch data values from DHIS2.

        Args:
            data_set_id:  Dataset UID.
            org_unit_ids: list of org unit UIDs, or a single UID string.
            period:       DHIS2 period (e.g. '202401', '2024Q1', '2024',
                          or relative like 'LAST_12_MONTHS').

        Returns:
            Parsed JSON dict. If empty response body, returns {"dataValues": []}.
        """
        endpoint = f"{self.api_url}/dataValueSets"
        params = {
            "dataSet": data_set_id,
            "period": period,
            "orgUnit": ",".join(org_unit_ids) if isinstance(org_unit_ids, list)
                       else org_unit_ids,
            "format": "json",
            "paging": "false",
        }
        logger.info(
            f"Fetching data: dataset={data_set_id}, "
            f"orgUnits={len(org_unit_ids) if isinstance(org_unit_ids, list) else 1}, "
            f"period={period}"
        )
        resp = self.session.get(endpoint, params=params, timeout=120)
        resp.raise_for_status()

        data = self._safe_json(
            resp, context=f"dataset={data_set_id}, period={period}"
        )
        count = len(data.get("dataValues", []))
        logger.info(f"Received {count} data values")
        return data

    # ------------------------------------------------------------------ #
    # Metadata — simplified versions (used by the ETL pipeline)
    # ------------------------------------------------------------------ #
    def get_datasets(self):
        """Fetch all datasets with minimal fields."""
        url = f"{self.api_url}/dataSets"
        params = {
            "paging": "false",
            "fields": "id,name,displayName,periodType,code",
        }
        logger.info(f"Fetching datasets from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return self._safe_json(resp, "datasets").get("dataSets", [])

    def get_all_datasets(self):
        """Alias for get_datasets (backward compat)."""
        return self.get_datasets()

    def get_data_elements(self):
        """
        Fetch data element metadata (id → {name, code, value_type}).
        Returns dict keyed by id.
        """
        url = f"{self.api_url}/dataElements"
        params = {
            "paging": "false",
            "fields": "id,displayName,name,code,valueType",
        }
        logger.info(f"Fetching data elements from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = self._safe_json(resp, "dataElements")
        return {
            de["id"]: {
                "name": de.get("displayName") or de.get("name"),
                "code": de.get("code"),
                "value_type": de.get("valueType"),
            }
            for de in data.get("dataElements", [])
        }

    def get_org_units(self):
        """
        Fetch org unit metadata (id → {name, code, path, level}).
        Returns dict keyed by id.
        """
        url = f"{self.api_url}/organisationUnits"
        params = {
            "paging": "false",
            "fields": "id,displayName,name,code,path,level",
        }
        logger.info(f"Fetching organisation units from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = self._safe_json(resp, "organisationUnits")
        return {
            ou["id"]: {
                "name": ou.get("displayName") or ou.get("name"),
                "code": ou.get("code"),
                "path": ou.get("path"),
                "level": ou.get("level"),
            }
            for ou in data.get("organisationUnits", [])
        }

    # ------------------------------------------------------------------ #
    # Metadata — FULL versions (used for metadata.* sync)
    # ------------------------------------------------------------------ #
    def get_datasets_full(self):
        """Fetch datasets with descriptions and period types."""
        url = f"{self.api_url}/dataSets"
        params = {
            "paging": "false",
            "fields": "id,name,displayName,description,periodType",
        }
        logger.info(f"Fetching full datasets from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return self._safe_json(resp, "dataSets_full").get("dataSets", [])

    def get_data_elements_full(self):
        """Fetch data elements with descriptions."""
        url = f"{self.api_url}/dataElements"
        params = {
            "paging": "false",
            "fields": "id,name,displayName,description,code,valueType",
        }
        logger.info(f"Fetching full data elements from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return self._safe_json(resp, "dataElements_full").get("dataElements", [])

    def get_org_units_full(self):
        """Fetch org units with hierarchy, level, and parent."""
        url = f"{self.api_url}/organisationUnits"
        params = {
            "paging": "false",
            "fields": "id,name,displayName,code,path,level,parent[id]",
        }
        logger.info(f"Fetching full organisation units from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return self._safe_json(resp, "organisationUnits_full").get("organisationUnits", [])

    def get_category_option_combos(self):
        """Fetch category option combos with resolved names."""
        url = f"{self.api_url}/categoryOptionCombos"
        params = {
            "paging": "false",
            "fields": "id,name,displayName",
        }
        logger.info(f"Fetching category option combos from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return self._safe_json(resp, "categoryOptionCombos").get("categoryOptionCombos", [])

    def get_category_combos(self):
        """Fetch category combos (the disaggregation definitions)."""
        url = f"{self.api_url}/categoryCombos"
        params = {
            "paging": "false",
            "fields": "id,name,displayName,categories[id,name]",
        }
        logger.info(f"Fetching category combos from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return self._safe_json(resp, "categoryCombos").get("categoryCombos", [])

    def get_data_element_category_combos(self):
        """
        Return {data_element_id: [category_combo_id, ...]} from DHIS2.
        Used to populate data_element_to_disaggregation bridge.
        """
        url = f"{self.api_url}/dataElements"
        params = {
            "paging": "false",
            "fields": "id,categoryCombo[id]",
        }
        logger.info(f"Fetching data element → category combo map")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = self._safe_json(resp, "de_to_cc")
        mapping = {}
        for de in data.get("dataElements", []):
            de_id = de.get("id")
            cc_id = (de.get("categoryCombo") or {}).get("id")
            if de_id and cc_id:
                mapping.setdefault(de_id, []).append(cc_id)
        return mapping

    # ------------------------------------------------------------------ #
    # Diagnostics
    # ------------------------------------------------------------------ #
    def ping(self):
        """Quick connectivity check. Returns True if the API responds."""
        try:
            resp = self.session.get(
                f"{self.api_url}/system/info", timeout=15
            )
            return resp.status_code == 200
        except Exception:
            return False

    def get_system_info(self):
        """Fetch DHIS2 system info (useful to confirm version)."""
        resp = self.session.get(f"{self.api_url}/system/info", timeout=30)
        resp.raise_for_status()
        return self._safe_json(resp, "system_info")
