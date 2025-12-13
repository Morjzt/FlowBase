import pandas as pd
import os
import chardet
from src.ingestion.ingest import IngestBase
from src.utils.logging import PipeLineLogger

class IngestLocal(IngestBase):
    def __init__(self):
        self.path = self.config.get("local_path", "./data/raw") 
        self.file_type = self.config.get("file_type", "csv")
        self.recursive = self.config.get("recursive", True) 
        self.logger = PipeLineLogger(self.__class__.__name__).get_logger()
        self.df = None
        self.processed_dir = "./data/processed_temp" 
        os.makedirs(self.processed_dir, exist_ok=True)


    def ingest(self) -> pd.DataFrame:
        df_list = []

        for root, _, files in os.walk(self.path):
            for f in files:
                if f.endswith(self.file_type):
                    source_file_path = os.path.join(root, f)
                    usable_file_path = self.process_file(source_file_path, self.processed_dir)

                    if usable_file_path:
                        try:
                            if self.file_type == "csv":
                                df = pd.read_csv(usable_file_path)
                            elif self.file_type == "parquet":
                                df = pd.read_parquet(usable_file_path)
                            
                            df_list.append(df)
                            self.logger.info(f"Successfully appended DataFrame from {usable_file_path}")

                        except Exception as e:
                            self.logger.error(f"Failed to read file {usable_file_path} into DataFrame: {e}")

            if not self.recursive:
                break
        
        if df_list:
            self.df = pd.concat(df_list, ignore_index=True)
            self.logger.info(f"Ingestion complete. Master DataFrame shape: {self.df.shape}")
        else:
            self.logger.warning("No files found or processed during ingestion.")

        return self.df


    # <---- Utility methods for encoding ---->

    def detect_encoding(self, path):
        with open(path, "rb") as f:
            raw_data = f.read(50000) 
            output = chardet.detect(raw_data)
            return output['encoding']

    def convert_to_utf8(self, source_path, source_encoding, target_path):
        try:
            with open(source_path, "r", encoding=source_encoding) as infile:
                content = infile.read()

            with open(target_path, "w", encoding="utf-8") as outfile:
                outfile.write(content) 

            self.logger.info(f"Successfully converted {source_path} to {target_path}")
            return True
        
        except UnicodeDecodeError:
            self.logger.error(f"Failed to decode {source_encoding} for file {source_path}")
            return False
        except Exception as e:
            self.logger.error(f"An unexpected error has occurred: {e}")
            return False

    def process_file(self, source_file_path, processed_dir):
        detected_enc = self.detect_encoding(source_file_path)
        
        if detected_enc.lower() != "utf-8":
            filename = os.path.basename(source_file_path)
            target_path = os.path.join(processed_dir, filename)
            
            self.logger.info(f"Encoding detected as {detected_enc}. Converting to UTF-8.")
            success = self.convert_to_utf8(source_file_path, detected_enc, target_path)
            
            if success:
                return target_path
            else:
                return None
        else:
            self.logger.info(f"File {source_file_path} is already UTF-8. Using as it is.")
            return source_file_path
