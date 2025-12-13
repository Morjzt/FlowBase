import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

class PipeLineLogger:
    def __init__(self, name="PipelineLogger", log_dir="logs", level=logging.INFO):
        self.name = name
        self.level = level
        self.log_dir = log_dir

        os.makedirs(self.log_dir, exist_ok=True)

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.level)

        if not self.logger.hasHandlers():
            self._add_handlers()


    def _add_handlers(self):
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        file_path = os.path.join(self.log_dir, f"{self.name}.log")

        rotating_handler = RotatingFileHandler(
            file_path, maxBytes=5_000_000, 
            backupCount=5
        )

        timed_handler = TimedRotatingFileHandler(
            when="midnight",
            backupCount=7
        )

        rotating_handler.setLevel(self.level)
        rotating_handler.setFormatter(formatter)
        self.logger.addHandler(rotating_handler)
        self.logger.addHandler(timed_handler)

    def get_logger(self):
        self.logger.info("Logging has been initialized.")
        return self.logger