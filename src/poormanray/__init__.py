"""
Poor Man's Ray
==============

A minimal alternative to Ray for distributed data processing on EC2 instances.
"""

from poormanray.logger import setup_logging
from poormanray.version import __version__

__all__ = ["__version__"]

logger = setup_logging()
