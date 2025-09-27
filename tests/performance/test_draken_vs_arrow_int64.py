
"""Performance comparison tests between Draken and Arrow for Int64 operations.

This module contains benchmarks that compare the performance of Draken's
Int64Vector operations against equivalent PyArrow operations. The tests
validate that Draken's optimized implementations achieve at least 2x
better performance than Arrow for common comparison operations.

Tests include:
- Less than comparisons
- Greater than comparisons  
- Equality comparisons
- Not equal comparisons
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import opteryx
from draken import Vector
from pyarrow import compute


def test_performance_less_than_int64():
    import time

    arr = opteryx.query_to_arrow("SELECT id FROM $satellites")["id"]
    vec = Vector.from_arrow(arr)

    start = time.perf_counter_ns()
    vec.less_than(10)
    draken_ms = (time.perf_counter_ns() - start) / 1e6

    start = time.perf_counter_ns()
    compute.less(arr, 10)
    arrow_ms = (time.perf_counter_ns() - start) / 1e6

    assert draken_ms < arrow_ms * 0.50, f"Draken {draken_ms:.3f}ms is not at least twice as fast as Arrow {arrow_ms:.3f}ms"


def test_performance_greater_than_int64():
    import time

    arr = opteryx.query_to_arrow("SELECT id FROM $satellites")["id"]
    vec = Vector.from_arrow(arr)

    start = time.perf_counter_ns()
    vec.greater_than(10)
    draken_ms = (time.perf_counter_ns() - start) / 1e6

    start = time.perf_counter_ns()
    compute.greater(arr, 10)
    arrow_ms = (time.perf_counter_ns() - start) / 1e6

    assert draken_ms < arrow_ms * 0.50, f"Draken {draken_ms:.3f}ms is not at least twice as fast as Arrow {arrow_ms:.3f}ms"

def test_performance_equal_int64():
    import time

    arr = opteryx.query_to_arrow("SELECT id FROM $satellites")["id"]
    vec = Vector.from_arrow(arr)

    start = time.perf_counter_ns()
    vec.equals(10)
    draken_ms = (time.perf_counter_ns() - start) / 1e6

    start = time.perf_counter_ns()
    compute.equal(arr, 10)
    arrow_ms = (time.perf_counter_ns() - start) / 1e6

    assert draken_ms < arrow_ms * 0.50, f"Draken {draken_ms:.3f}ms is not at least twice as fast as Arrow {arrow_ms:.3f}ms"

def test_performance_not_equal_int64():
    import time

    arr = opteryx.query_to_arrow("SELECT id FROM $satellites")["id"]
    vec = Vector.from_arrow(arr)

    start = time.perf_counter_ns()
    vec.not_equals(10)
    draken_ms = (time.perf_counter_ns() - start) / 1e6

    start = time.perf_counter_ns()
    compute.not_equal(arr, 10)
    arrow_ms = (time.perf_counter_ns() - start) / 1e6

    assert draken_ms < arrow_ms * 0.50, f"Draken {draken_ms:.3f}ms is not at least twice as fast as Arrow {arrow_ms:.3f}ms"


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()