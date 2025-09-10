import pyarrow

from draken.src.draken_fixed_width import FixedColumn
from draken.src.draken_fixed_width import from_arrow_fixed

__all__ = ["DrakenArray"]


class DrakenArray(FixedColumn):
    @classmethod
    def from_arrow(cls, arr: pyarrow.Array) -> "DrakenArray":
        base = from_arrow_fixed(arr)
        darr = cls(base.dtype, base.length)
        return darr
