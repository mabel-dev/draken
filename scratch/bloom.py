import os
import sys

from draken.compiled.bloom_filter import BloomFilter
from draken.compiled.bloom_filter import deserialize

sys.path.insert(1, os.path.join(sys.path[0], ".."))


data = {
    "term1": {"doc_id": 1, "frequency": 3, "positions": [1, 5, 9]},
    "term2": {"doc_id": 2, "frequency": 2, "positions": [3, 7]},
    "term3": {"doc_id": 1, "frequency": 1, "positions": [4]},
    "term4": {"doc_id": 3, "frequency": 4, "positions": [2, 6, 8, 10]},
    "term5": {"doc_id": 2, "frequency": 1, "positions": [11]},
}

bf = BloomFilter()

for rec in data.keys():
    bf.add(hash(rec))

ser = bf.serialize()
print(bytes(ser))
de = deserialize(ser)

print(de.possibly_contains(hash("term2")))
print(de.possibly_contains(hash("term9")))
