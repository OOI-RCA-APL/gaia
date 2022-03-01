import logging
from logging import Logger
from logging.config import dictConfig
from typing import Any, Dict, Optional

from pydantic import BaseModel


class LogConfig(BaseModel):
    """
    Common logging configuration.
    """

    level: str = "INFO"
    """
    Set a log level for loggers.
    """


class LoggingState:
    config = LogConfig()
    loggers: Dict[str, Logger] = {}


state = LoggingState


def setup(config: Optional[LogConfig] = None) -> None:
    """
    Set up logging globally.

    :param config: Configuration options to apply.
    """
    if config:
        state.config = config

    dictConfig(_generate_config())


def main() -> Logger:
    """
    Get the main logger. This should be used in mainline application code.
    """
    return logging.getLogger("uvicorn")


def get(name: str) -> Logger:
    """
    Get a named logger. This can be used to separate logging for different parts of application
    code.

    :param name: The name of the logger. This will be displayed alongside any logged messages.
    """
    logger = logging.getLogger(name)
    if name not in state.loggers:
        state.loggers[name] = logger
        setup()

    return logger


def _generate_config() -> Dict[str, Any]:
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "[%(asctime)s] [%(process)s] [%(levelname)s] [%(name)s] %(message)s",
            },
            "uvicorn": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "[%(asctime)s] [%(process)s] [%(levelname)s] %(message)s",
            },
            "uvicorn.access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": "[%(asctime)s] [%(process)s] [%(levelname)s] [%(client_addr)s] - %(request_line)s - %(status_code)s",
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "stream": "ext://sys.stdout",
            },
            "uvicorn": {
                "class": "logging.StreamHandler",
                "formatter": "uvicorn",
                "stream": "ext://sys.stdout",
            },
            "uvicorn.access": {
                "class": "logging.StreamHandler",
                "formatter": "uvicorn.access",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            **(
                {
                    name: {
                        "level": state.config.level,
                        "handlers": ["default"],
                    }
                    for name in state.loggers.keys()
                }
            ),
            "uvicorn": {
                "level": state.config.level,
                "handlers": ["uvicorn"],
            },
            "uvicorn.access": {
                "level": state.config.level,
                "propagate": False,
                "handlers": ["uvicorn.access"],
            },
        },
    }
