# src/ingestion/ingest_s3.py
import boto3
import os
import pandas as pd
from src.ingestion.ingest import IngestBase, logger

class IngestS3(IngestBase):
    def ingest(self) -> pd.DataFrame:
        s3_cfg = self.config
        bucket = s3_cfg.get("s3_bucket")
        prefix = s3_cfg.get("s3_prefix", "")
        file_type = s3_cfg.get("file_type", "csv")

        session = boto3.Session(
            aws_access_key_id=s3_cfg["aws"]["access_key_id"],
            aws_secret_access_key=s3_cfg["aws"]["secret_access_key"],
            region_name=s3_cfg["aws"]["region_name"]
        )
        s3 = session.client("s3")

        df_list = []

        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            for obj in response.get("Contents", []):
                key = obj["Key"]
                if key.endswith(file_type):
                    obj_data = s3.get_object(Bucket=bucket, Key=key)
                    df = pd.read_csv(obj_data["Body"])
                    df_list.append(df)
            return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
        except Exception as e:
            logger.error(f"S3 ingestion failed: {e}")
            return pd.DataFrame()
