import pandas as pd
from abc import ABC, abstractmethod
from src.utils.logging import PipeLineLogger

class IngestBase:
    def __init__(self, config: dict):
        self.logger = PipeLineLogger(self.__class__.__name__).get_logger()
        self.config = config


    @abstractmethod
    def ingest(self) -> pd.DataFrame: 
        pass 

    def read_file(self, path: str, file_type: str) -> pd.DataFrame:
        try:
            if file_type.lower() =="csv":
                return pd.read_csv(path)
        
            elif file_type.lower() == "parquet":
                return pd.read_parquet(path)
        
            else:
                raise ValueError(f"Unsupported file type {file_type}")
        
        except Exception as e:
            self.logger.error(f"Error reading file {path}: {e}")
            return pd.DataFrame