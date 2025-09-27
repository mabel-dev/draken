"""Draken: Cython/Arrow Interoperability Library.

This package provides efficient columnar data structures and algorithms
with zero-copy interoperability with Apache Arrow. It includes:
- Vector classes for different data types (int64, float64, string, bool)
- Morsel data structures for batch processing
- Arrow integration for seamless data exchange

Main exports:
- Vector: Base vector class for columnar data
- Morsel: Batch data processing container
"""

from draken.__version__ import __author__
from draken.__version__ import __build__
from draken.__version__ import __version__
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector

__all__ = ("Vector", "Morsel", "__version__", "__build__", "__author__")
