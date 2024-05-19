import os
import sys

import ormsgpack
import zstd

from draken.compiled import create_sstable
from draken.compiled.hadro import lookup_eq
from draken.compiled.hadro import lookup_in_list
from draken.compiled.hadro import lookup_range

sys.path.insert(1, os.path.join(sys.path[0], ".."))


def generate_sample_data():
    data = {
        "term1": {"doc_id": 1, "frequency": 3, "positions": [1, 5, 9]},
        "term2": {"doc_id": 2, "frequency": 2, "positions": [3, 7]},
        "term3": {"doc_id": 1, "frequency": 1, "positions": [4]},
        "term4": {"doc_id": 3, "frequency": 4, "positions": [2, 6, 8, 10]},
        "term5": {"doc_id": 2, "frequency": 1, "positions": [11]},
    }
    return data


sample_data = generate_sample_data()


sstable_bytes = create_sstable(sample_data)
print(f"SSTable created with size: {len(sstable_bytes)} bytes")

term_to_lookup = "term2"
result = lookup_eq(sstable_bytes, term_to_lookup)
if result:
    print(f"Lookup result for '{term_to_lookup}': {result}")
else:
    print(f"'{term_to_lookup}' not found in SSTable")

terms_to_lookup = ["term1", "term3", "term6"]  # 'term6' does not exist
results = lookup_in_list(sstable_bytes, terms_to_lookup)
print(f"Lookup results for terms {terms_to_lookup}: {results}")

key_to_lookup = "term3"
comparison = "GT"
results = lookup_range(sstable_bytes, key_to_lookup, comparison)
print(f"Lookup range '{comparison}' results for '{key_to_lookup}': {results}")
