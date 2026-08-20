import logging
from pathlib import Path

LOG_FILE = Path(__file__).resolve().with_name("pipeline_upload.log")


def set_up_log():
    logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',                   
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
