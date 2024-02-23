import logging
from pathlib import Path

# Formatting
FORMAT = "%(levelname)s\t[%(asctime)s]\t[%(filename)s:%(funcName)s:%(lineno)d]\t%(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(FORMAT, DATE_FORMAT)

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add stream handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


# Function to get file handler
def get_log_file_handler(log_file: Path):
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    return file_handler
