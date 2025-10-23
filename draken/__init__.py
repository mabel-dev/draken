"""Draken: Cython/Arrow Interoperability Library.

This package provides efficient columnar data structures and algorithms
with zero-copy interoperability with Apache Arrow. It includes:
- Vector classes for different data types (int64, float64, string, bool)
- Morsel data structures for batch processing
- Arrow integration for seamless data exchange
- Compiled expression evaluators for high-performance query evaluation

Main exports:
- Vector: Base vector class for columnar data
- Morsel: Batch data processing container
- evaluate: Compiled expression evaluator
"""

from draken.__version__ import __author__
from draken.__version__ import __build__
from draken.__version__ import __version__
from draken.morsels.morsel import Morsel
from draken.vectors.vector import Vector

# Import evaluator module - use lazy import to avoid circular dependencies
def evaluate(morsel, expression):
    """
    Evaluate an expression tree over a morsel with compiled optimization.
    
    Args:
        morsel: Morsel to evaluate over
        expression: Expression tree (use draken.evaluators.expression classes)
        
    Returns:
        Vector: Result of evaluation
    """
    from draken.evaluators.evaluator import evaluate as _evaluate
    return _evaluate(morsel, expression)

__all__ = ("Vector", "Morsel", "evaluate", "__version__", "__build__", "__author__")
