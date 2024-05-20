# cython: language_level=3

from typing import List, Tuple, Dict
from libc.stdint cimport int32_t, int64_t

cdef class StringBinaryIndex:
    cdef list key_store
    cdef list value_store
    cdef dict key_to_pointer

    def __init__(self):
        self.key_store = []
        self.value_store = []
        self.key_to_pointer = {}

    def add_entry(self, key: str, filename: str, offset: int):
        # Add individual (filename, offset) tuples for the given key
        if key not in self.key_to_pointer:
            self.key_to_pointer[key] = len(self.value_store)
            self.key_store.append((len(key.encode('utf-8')), key.encode('utf-8'), self.key_to_pointer[key]))
            self.value_store.append([])  # Initialize empty list for new key

        # Add value to the corresponding value list
        pointer = self.key_to_pointer[key]
        self.value_store[pointer].append((len(filename.encode('utf-8')), filename.encode('utf-8'), offset))

    def finalize_index(self):
        # Sort key_store by keys for binary search
        self.key_store.sort()

    def lookup_eq(self, key: str) -> List[Tuple[str, int]]:
        # Perform binary search on key_store
        pointer = self.key_to_pointer.get(key)
        if pointer is None:
            return []

        value_data = self.value_store[pointer]
        return [(filename.decode('utf-8'), offset) for _, filename, offset in value_data]

    def lookup_in_list(self, keys: List[str]) -> Dict[str, List[Tuple[str, int]]]:
        result = {}
        for key in keys:
            result[key] = self.lookup_eq(key)
        return result

    def lookup_range(self, start_key: str, end_key: str) -> Dict[str, List[Tuple[str, int]]]:
        result = {}
        start_index = self._binary_search(start_key, find_start=True)
        end_index = self._binary_search(end_key, find_start=False)

        for index in range(start_index, end_index + 1):
            key_len, key_bytes, pointer = self.key_store[index]
            key = key_bytes.decode('utf-8')
            result[key] = self.lookup_eq(key)
        
        return result

    def _binary_search(self, key: str, find_start: bool) -> int:
        # Implement binary search on the sorted key_store
        key_bytes = key.encode('utf-8')
        low, high = 0, len(self.key_store) - 1
        while low <= high:
            mid = (low + high) // 2
            mid_key_bytes = self.key_store[mid][1]
            if mid_key_bytes < key_bytes:
                low = mid + 1
            elif mid_key_bytes > key_bytes:
                high = mid - 1
            else:
                if find_start:
                    if mid == 0 or self.key_store[mid - 1][1] != key_bytes:
                        return mid
                    high = mid - 1
                else:
                    if mid == len(self.key_store) - 1 or self.key_store[mid + 1][1] != key_bytes:
                        return mid
                    low = mid + 1
        return low if find_start else high
