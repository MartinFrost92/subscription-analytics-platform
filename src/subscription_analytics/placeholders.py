"""Executable smoke task; replace only after defining data contracts."""
import logging


def placeholder(stage: str) -> None:
    """Confirm Python execution without reading or writing business data."""
    logging.getLogger(__name__).info("%s placeholder executed successfully", stage)
