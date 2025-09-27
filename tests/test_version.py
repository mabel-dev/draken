"""Version information tests for Draken.

This module tests that the Draken package properly exposes version information
including the version string, build number, and author information. These
attributes are essential for package identification and debugging.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import draken


def test_version():
    assert hasattr(draken, "__version__")
    print("__version__", draken.__version__)
    assert hasattr(draken, "__build__")
    print("__build__", draken.__build__)
    assert hasattr(draken, "__author__")
    print("__author__", draken.__author__)


if __name__ == "__main__":  # pragma: no cover
    test_version()

    print("okay")
