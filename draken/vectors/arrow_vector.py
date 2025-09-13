import pyarrow
import pyarrow.compute as pc

from draken.vectors.draken_vector import DrakenVector


class ArrowVector(DrakenVector):
    """
    Fallback DrakenVector implementation backed by a pyarrow.Array.
    This is used for types that don't yet have a native DrakenVector.

    - Wraps any pyarrow.Array
    - Methods mirror DrakenVector API
    - Delegates to pyarrow.compute
    """

    def __init__(self, arr: pyarrow.Array):
        if not isinstance(arr, pyarrow.Array):
            raise TypeError("ArrowBackedVector requires a pyarrow.Array")
        self._arr = arr

    # -------- Core metadata --------
    @property
    def length(self) -> int:
        return len(self._arr)

    @property
    def dtype(self):
        return self._arr.type

    @property
    def itemsize(self):
        try:
            return self._arr.type.bit_width // 8
        except Exception:
            return None

    def to_arrow(self):
        return self._arr

    # -------- Generic operations --------
    def take(self, indices):
        indices_arr = pyarrow.array(indices, type=pyarrow.int32())
        out = pc.take(self._arr, indices_arr)
        return ArrowVector(out)

    def equals(self, value):
        return pc.equal(self._arr, value).to_numpy().astype("int8")

    def not_equals(self, value):
        return pc.not_equal(self._arr, value).to_numpy().astype("int8")

    def greater_than(self, value):
        return pc.greater(self._arr, value).to_numpy().astype("int8")

    def greater_than_or_equals(self, value):
        return pc.greater_equal(self._arr, value).to_numpy().astype("int8")

    def less_than(self, value):
        return pc.less(self._arr, value).to_numpy().astype("int8")

    def less_than_or_equals(self, value):
        return pc.less_equal(self._arr, value).to_numpy().astype("int8")

    def sum(self):
        return pc.sum(self._arr).as_py()

    def min(self):
        return pc.min(self._arr).as_py()

    def max(self):
        return pc.max(self._arr).as_py()

    def is_null(self):
        return pc.is_null(self._arr).to_numpy().astype("int8")

    def to_pylist(self):
        return self._arr.to_pylist()

    def hash(self):
        # Arrow has experimental hash kernels; fallback: use Python's hash
        try:
            return pc.hash(self._arr).to_numpy().astype("uint64")
        except Exception:
            return [hash(v) if v is not None else 0 for v in self._arr.to_pylist()]

    def __str__(self):
        return f"<ArrowVector type={self._arr.type} len={len(self._arr)} values={self._arr.to_pylist()[:10]}>"


# convenience
def from_arrow(array: pyarrow.Array) -> ArrowVector:
    return ArrowVector(array)
