import sys
from pathlib import Path

from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO")

BASE_DIR = Path(__file__).parent.parent
