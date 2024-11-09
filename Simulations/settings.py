from pathlib import Path
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent
logger.debug(BASE_DIR)

