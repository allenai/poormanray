import logging
import os


def setup_logging(logging_level: int | None = None):
    logging_level = (
        logging_level or getattr(logging, str(os.environ.get("PMR_LOG_LEVEL", "")).upper(), None) or logging.INFO
    )
    if logging_level is None:
        raise ValueError("Invalid logging level")

    logger = logging.getLogger(__name__)
    logger.setLevel(logging_level)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging_level)
    formatter = logging.Formatter("[%(levelname)s][%(asctime)s] %(message)s", datefmt="%H:%M:%S")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger
