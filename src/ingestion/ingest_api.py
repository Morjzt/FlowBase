# src/ingestion/ingest_api.py
import requests
import pandas as pd
from src.ingestion.ingest import IngestBase
from src.utils.logging import PipeLineLogger

class IngestAPI(IngestBase):
    def ingest(self) -> pd.DataFrame:
        api_cfg = self.config.get("api", {})
        base_url = api_cfg.get("base_url")
        endpoint = api_cfg.get("endpoint")
        auth_token = api_cfg.get("auth_token")
        timeout = api_cfg.get("timeout", 30)
        self.logger = PipeLineLogger(self.__class__.__name__).get_logger()

        headers = {"Authorization": f"Bearer {auth_token}"}
        url = f"{base_url}{endpoint}"

        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            return pd.DataFrame(data)
        except Exception as e:
            self.logger.error(f"API ingestion failed: {e}")
            return pd.DataFrame()
