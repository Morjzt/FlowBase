import time
import json
import sys
import pandas as pd
import requests
from typing import Optional
from src.utils.logging import PipeLineLogger
from src.ingestion.ingest import IngestBase


MAX_PAYLOAD_BYTES = 5 * 1024 * 1024 
MAX_RECORDS_PER_BATCH = 10_000
MAX_JSON_DEPTH = 5


class IngestAPI(IngestBase):
    def ingest(self) -> pd.DataFrame:
        self.logger = PipeLineLogger(self.__class__.__name__).get_logger()
        cfg = self.config.get("api", {})

        base_url = cfg.get("base_url")
        endpoint = cfg.get("endpoint")
        auth_token = cfg.get("auth_token")

        if not base_url.startswith("https://"):
            raise ValueError("API base url must use HTTPS")

        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Accept": "application/json",
            "User-Agent": "InventoryPipeline/1.0"
        }

        page = 1
        page_size = cfg.get("page_size", 100)
        max_retries = cfg.get("max_retries", 5)
        timeout = cfg.get("timeout", 10)

        all_rows = []

        while True:
            response = self._safe_request(
                url=f"{base_url}{endpoint}",
                headers=headers,
                params={"page": page, "limit": page_size},
                timeout=timeout,
                max_retries=max_retries
            )

            if response is None:
                break

            if not self._validate_payload_size(response):
                self.logger.error("Payload too large — aborting ingestion")
                break

            try:
                payload = response.json()
            except json.JSONDecodeError:
                self.logger.error("Invalid JSON payload")
                break

            if not self._validate_json_depth(payload):
                self.logger.error("JSON payload too deeply nested — aborting")
                break

            records = self._extract_records(payload)

            if not records:
                self.logger.info("No more records found — ingestion complete")
                break

            if len(records) > MAX_RECORDS_PER_BATCH:
                self.logger.error("Batch size exceeds safety limits")
                break

            validated = self._validate_schema(records)
            all_rows.extend(validated)

            self.logger.info(
                f"Fetched page {page} | records: {len(validated)}"
            )

            page += 1

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        self.logger.info(f"Ingestion finished => shape: {df.shape}")
        return df


    def _safe_request(
        self,
        url: str,
        headers: dict,
        params: dict,
        timeout: int,
        max_retries: int
    ) -> Optional[requests.Response]:

        backoff = 1

        for attempt in range(1, max_retries + 1):
            try:
                r = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=timeout
                )

                if r.status_code == 401:
                    self.logger.error("Unauthorized API access")
                    return None

                if r.status_code == 429:
                    retry_after = int(r.headers.get("Retry-After", backoff))
                    self.logger.warning(f"Rate limited — sleeping {retry_after}s")
                    time.sleep(retry_after)
                    continue

                if 400 <= r.status_code < 500:
                    self.logger.error(f"Client error {r.status_code}")
                    return None

                r.raise_for_status()
                return r

            except requests.exceptions.RequestException as e:
                self.logger.warning(
                    f"Network error (attempt {attempt}/{max_retries}): {e}"
                )
                time.sleep(backoff)
                backoff = min(backoff **2, 30)
                return True

        self.logger.error("Max retries exceeded")
        return None


    def _validate_payload_size(self, response: requests.Response) -> bool:
        size = len(response.content)
        return size <= MAX_PAYLOAD_BYTES

    def _validate_json_depth(self, obj, depth=0) -> bool:
        if depth > MAX_JSON_DEPTH:
            return False
        if isinstance(obj, dict):
            return all(self._validate_json_depth(v, depth + 1) for v in obj.values())
        if isinstance(obj, list):
            return all(self._validate_json_depth(v, depth + 1) for v in obj)
        return True

    def _extract_records(self, payload):
        if isinstance(payload, dict):
            return payload.get("data", [])
        if isinstance(payload, list):
            return payload
        return []

    def _validate_schema(self, records):
        required_fields = {"sku", "quantity"}

        valid = []
        for r in records:
            if not isinstance(r, dict):
                continue
            if not required_fields.issubset(r.keys()):
                self.logger.warning("Record missing required fields — dropped")
                continue
            valid.append(r)

        return valid