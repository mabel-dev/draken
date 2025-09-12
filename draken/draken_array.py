import pyarrow

from draken.vector.fixed_vector import FixedVector
from draken.vector.fixed_vector import from_arrow_fixed

__all__ = ["DrakenArray"]


class DrakenArray(FixedVector):
    @classmethod
    def from_arrow(cls, arr: pyarrow.Array) -> "DrakenArray":
        base = from_arrow_fixed(arr)
        darr = cls(base.dtype, base.length)
        return darr
