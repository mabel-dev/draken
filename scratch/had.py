import os
import sys

import opteryx
import ormsgpack
import zstd

from draken.compiled import create_sstable
from draken.compiled import match_equals
from draken.compiled import murmurhash3

sys.path.insert(1, os.path.join(sys.path[0], ".."))


def generate_sample_data():
    data = {}
    df = opteryx.query("SELECT name FROM $astronauts")
    for i, r in enumerate(df):
        data[r[0]] = [("$astronauts", i)]

    return data


sample_data = generate_sample_data()


sstable_bytes = create_sstable(sample_data, {}, 0)
print(f"SSTable created with size: {len(sstable_bytes)} bytes")

term_to_lookup = b"Anthony W. England"
result = match_equals(sstable_bytes, term_to_lookup)
if result:
    print(f"Lookup result for '{term_to_lookup}': {result}")
else:
    print(f"'{term_to_lookup}' not found in SSTable")
