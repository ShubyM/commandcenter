import logging
import logging.config
import pathlib

import yaml

from commandcenter.config import CC_DEBUG_MODE
from commandcenter.config.logging import CC_LOGGING_CONFIG_PATH



DEFAULT_LOGGING_SETTINGS_PATH = pathlib.Path(__file__).parent / "logging.yml"


def confgire_logging() -> None:
    """Configure logging for this runtime."""
    # If the user has specified a logging path and it exists we will ignore the
    # default entirely rather than dealing with complex merging
    path = (
        CC_LOGGING_CONFIG_PATH
        if CC_LOGGING_CONFIG_PATH and CC_LOGGING_CONFIG_PATH.exists()
        else DEFAULT_LOGGING_SETTINGS_PATH
    )
    config = yaml.safe_load(path.read_text())
    logging.config.dictConfig(config)

    if CC_DEBUG_MODE:
        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        for logger in loggers:
            logger.setLevel(level=logging.DEBUG)
        logging.getLogger().setLevel(level=logging.DEBUG)