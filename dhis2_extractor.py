"""
DHIS2 API client.

Targets DHIS2 2.30 (unversioned /api/ endpoints).
Handles retries, authentication, and metadata / data value fetching.
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
    """
    Client for a DHIS2 instance.

    Usage:
        ex = DHIS2Extractor()                       # DHIS2 PIH (default)
        ex = DHIS2Extractor(url="https://hmis.gov.ls/dhis",
                            username="duzoap", password="Demo@1234")  # DHIS2 MOH
    """

    def __init__(self, url=None, username=None, password=None):
        """
        Args:
            url:      DHIS2 base URL. Defaults to config.DHIS2_URL.
            username: DHIS2 username. Defaults to config.DHIS2_USERNAME.
            password: DHIS2 password. Defaults to config.DHIS2_PASSWORD.
        """
        self.base_url = (url or config.DHIS2_URL).rstrip("/")
        # DHIS2 2.30 does not support versioned endpoints for metadata
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
            Parsed JSON dict from /api/dataValueSets.
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
        data = resp.json()
        logger.info(f"Received {len(data.get('dataValues', []))} data values")
        return data

    # ------------------------------------------------------------------ #
    # Metadata — simplified versions (used by the ETL pipeline)
    # ------------------------------------------------------------------ #
    def get_datasets(self):
        """
        Fetch all datasets with minimal fields (used by full_extractor).
        Returns list of dicts.
        """
        url = f"{self.api_url}/dataSets"
        params = {
            "paging": "false",
            "fields": "id,name,displayName,periodType,code",
        }
        logger.info(f"Fetching datasets from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return resp.json().get("dataSets", [])

    # Alias kept for backwards compatibility with earlier code
    def get_all_datasets(self):
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
        return {
            de["id"]: {
                "name": de.get("displayName") or de.get("name"),
                "code": de.get("code"),
                "value_type": de.get("valueType"),
            }
            for de in resp.json().get("dataElements", [])
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
        return {
            ou["id"]: {
                "name": ou.get("displayName") or ou.get("name"),
                "code": ou.get("code"),
                "path": ou.get("path"),
                "level": ou.get("level"),
            }
            for ou in resp.json().get("organisationUnits", [])
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
        return resp.json().get("dataSets", [])

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
        return resp.json().get("dataElements", [])

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
        return resp.json().get("organisationUnits", [])

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
        return resp.json().get("categoryOptionCombos", [])

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
        return resp.json()

    def get_data_element_category_combos(self):
        """Return {data_element_id: [category_combo_id, ...]} from DHIS2."""
        url = f"{self.api_url}/dataElements"
        params = {"paging": "false", "fields": "id,categoryCombo[id]"}
        logger.info(f"Fetching data element → category combo map")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        mapping = {}
        for de in resp.json().get("dataElements", []):
            de_id = de.get("id")
            cc = (de.get("categoryCombo") or {}).get("id")
            if de_id and cc:
               mapping.setdefault(de_id, []).append(cc)
        return mapping

    def get_category_combos(self):
        """Fetch all category combos (the disaggregation definitions)."""
        url = f"{self.api_url}/categoryCombos"
        params = {
            "paging": "false",
            "fields": "id,name,displayName,categories[id,name]",
        }
        logger.info(f"Fetching category combos from {url}")
        resp = self.session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        return resp.json().get("categoryCombos", [])
