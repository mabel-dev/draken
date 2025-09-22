import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

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
