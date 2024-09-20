"""
write_table
load_table -> DrakenTable

"""

import datetime
from dataclasses import dataclass
from typing import Any
from typing import BinaryIO
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union, Dict

from orso.schema import RelationSchema
import orso


@dataclass
def DrakenMetadata():
    num_rows: int
    num_columns: int
    version: int
    created: datetime.datetime
    schema: RelationSchema

    def shape(self):
        return (self.num_rows, self.num_columns)


def DrakenTable():

    @property
    def metadata(self):
        pass

    def __iter__(self):
        pass

    def pylist(self) -> List[Dict[str, Any]]:
        pass

    def arrow(self):
        pass

    def orso(self) -> orso.DataFrame:
        pass

def read_table(
    self,
    filelike: Union[str, bytes, memoryview, BinaryIO],
    columns: Optional[List[str]] = None,
    filter: Optional[Union[Tuple[str, str, Any], List[Tuple]]] = None,
    offsets: Optional[List[int]] = None
) -> DrakenTable:
    pass