import datetime
import os
from loguru import logger


def log(*values: object, flush: bool = False):
    logger.info(*values)