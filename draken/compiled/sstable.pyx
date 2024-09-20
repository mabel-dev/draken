# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: nonecheck=False
# cython: overflowcheck=False
# cython: cdivision=True

"""
SSTable Module

This module provides functionality for creating and querying SSTable (Sorted String Table) files.
An SSTable is a persistent, immutable data structure commonly used in NoSQL databases and search engines 
to store large amounts of sorted data efficiently.

Structure of an SSTable:
------------------------
1. Header:
    - Metadata about the SSTable, including version, creation timestamp, offsets to various blocks, 
      number of keys, endianness, format flags, and a checksum for integrity verification.
    - Fixed size: 42 bytes.

2. Schema Block:
    - Describes the structure of the data stored in the SSTable using a JSON document.
    - Allows the file to be self-describing.

3. Bloom Filter:
    - A probabilistic data structure used to quickly check if a key might exist in the SSTable.
    - Reduces unnecessary binary search operations by filtering out non-existent keys.

4. Key Block:
    - Stores all unique keys in a sorted order and their corresponding offsets in the data block.
    - Each key entry is fixed size, allowing for efficient binary search operations.

5. Data Block:
    - Stores the actual values associated with each key.
    - The entire data block is compressed together for efficient storage and retrieval.

Functionality:
--------------
1. `create_sstable(data: dict, schema: dict, format_flags: uint8_t) -> bytes`:
    - Creates an SSTable from the given data and schema, serializes and compresses the necessary blocks, 
      and returns the serialized SSTable as bytes.

2. `match_equals(sstable: bytes, value: bytes) -> list`:
    - Searches for the exact match of a key in the SSTable.
    - Utilizes the Bloom filter for quick membership testing and performs a binary search on the key block.
    - Returns the deserialized value associated with the key if found, otherwise returns an empty list.

Supporting Classes:
-------------------
1. `Header`:
    - Represents the header of the SSTable, including methods for serialization and deserialization.

2. `BloomFilter`:
    - Represents the Bloom filter used for quick membership testing, including methods for adding items, 
      checking membership, and serialization/deserialization.

3. `KeyEntry`:
    - Represents an entry in the key block, including methods for serialization and deserialization.

4. `DataEntry`:
    - Represents an entry in the data block, including methods for serialization and deserialization.

5. `murmurhash3_32`:
    - An inlinable version of the MurmurHash3 algorithm used for hashing keys in the Bloom filter.
"""

from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t

import struct
import zlib
import time
import zstd
import ormsgpack
import numpy
from cpython cimport PyUnicode_AsUTF8String
from cpython.mem cimport PyMem_Malloc, PyMem_Free
from hashlib import sha256
from libc.stdlib cimport malloc, free
from libc.string cimport memset
from .bloom_filter cimport create_bloom_filter
from .bloom_filter cimport deserialize as deserialize_bloom_filter
from .murmurhash3_32 cimport cy_murmurhash3

cdef uint32_t HEADER_SIZE = 48
cdef uint32_t KEY_ENTRY_SIZE = 72

cdef class Header:
    cdef public char file_marker[6]  # File marker "DRAKEN"
    cdef public uint32_t version
    cdef public uint64_t creation_timestamp
    cdef public uint32_t schema_offset
    cdef public uint32_t schema_size
    cdef public uint32_t bloom_filter_offset
    cdef public uint32_t key_block_offset
    cdef public uint32_t data_block_offset
    cdef public uint32_t num_keys
    cdef public uint8_t endianness
    cdef public uint32_t checksum
    cdef public uint8_t format_flags

    def __init__(self, version: uint32_t, creation_timestamp: uint64_t, schema_offset: uint32_t,
                 schema_size: uint32_t, bloom_filter_offset: uint32_t, key_block_offset: uint32_t,
                 data_block_offset: uint32_t, num_keys: uint32_t, endianness: uint8_t, format_flags: uint8_t):
        self.set_file_marker()
        self.version = version
        self.creation_timestamp = creation_timestamp
        self.schema_offset = schema_offset
        self.schema_size = schema_size
        self.bloom_filter_offset = bloom_filter_offset
        self.key_block_offset = key_block_offset
        self.data_block_offset = data_block_offset
        self.num_keys = num_keys
        self.endianness = endianness
        self.checksum = 0  # to be computed later
        self.format_flags = format_flags

    cdef void set_file_marker(self):
        cdef bytes marker = b'DRAKEN'
        for i in range(6):
            self.file_marker[i] = marker[i]

    def serialize(self) -> bytes:
        return struct.pack('<6s I Q I I I I I I B B I',
                           bytes(self.file_marker),
                           self.version,
                           self.creation_timestamp,
                           self.schema_offset,
                           self.schema_size,
                           self.bloom_filter_offset,
                           self.key_block_offset,
                           self.data_block_offset,
                           self.num_keys,
                           self.endianness,
                           self.format_flags,
                           self.checksum)

    @staticmethod
    def deserialize(data: bytes):
        header = Header.__new__(Header)
        (header.file_marker, header.version, header.creation_timestamp, header.schema_offset, header.schema_size,
         header.bloom_filter_offset, header.key_block_offset, header.data_block_offset,
         header.num_keys, header.endianness, header.format_flags, header.checksum) = struct.unpack('<6s I Q I I I I I I B B I', data)
        return header

    def compute_checksum(self, key_block: bytes, data_block: bytes):
        combined_data = key_block + data_block
        self.checksum = cy_murmurhash3(<const void*>combined_data, len(combined_data), 0)

cdef class KeyEntry:
    cdef public uint32_t key_length
    cdef public bytes key
    cdef public uint32_t data_block_offset

    def __init__(self, key: bytes, data_block_offset: uint32_t):
        self.key_length = min(len(key), 64)  # Cap key length to 64 characters
        self.key = key[:64]
        self.data_block_offset = data_block_offset

    def serialize(self) -> bytes:
        return struct.pack('<I64sI', self.key_length, self.key.ljust(64, b'\x00'), self.data_block_offset)

    @staticmethod
    cdef inline KeyEntry deserialize(const unsigned char[::1] data):
        cdef uint32_t key_length
        cdef uint32_t data_block_offset
        cdef bytes key

        # Extract key_length
        key_length = data[0] | (data[1] << 8) | (data[2] << 16) | (data[3] << 24)

        # Extract data_block_offset
        data_block_offset = data[68] | (data[69] << 8) | (data[70] << 16) | (data[71] << 24)

        # Extract key and strip null bytes
        key = bytes(data[4:68]).rstrip(b'\x00')

        return KeyEntry(key, data_block_offset)


cdef class DataEntry:
    cdef uint32_t data_length
    cdef public bytes data

    def __init__(self, data: Union[bytes, memoryview]):
        self.data = bytes(data)
        self.data_length = len(self.data)

    def serialize(self) -> bytes:
        return struct.pack('<I', self.data_length) + self.data

    @staticmethod
    def deserialize(data: Union[bytes, memoryview]):
        """
        Deserialize the given data into a DataEntry object.

        Parameters:
            data (bytes or memoryview): The data to deserialize.

        Returns:
            DataEntry: The deserialized DataEntry object.
        """
        # Directly convert the first 4 bytes to an integer
        data_length = int.from_bytes(data[:4], 'little')
        raw_data = data[4:4 + data_length]
        return DataEntry(raw_data)


def serialize_schema(schema: dict) -> bytes:
    """
    We store the schema as a JSON document.
    """
    import orjson
    schema_json = orjson.dumps(schema)
    return schema_json

def deserialize_schema(schema_bytes: bytes) -> dict:
    import orjson
    return orjson.loads(schema_bytes)

def default_serializer(obj):
    """
    Types orjson doesn't know how to serialize but we want to support.
    """
    if isinstance(obj, numpy.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def create_sstable(dict data, dict schema, uint8_t format_flags) -> bytes:

    # Define header
    creation_timestamp = int(time.time())
    header = Header(version=1, creation_timestamp=creation_timestamp, schema_offset=0,
                    schema_size=0, bloom_filter_offset=0, key_block_offset=0,
                    data_block_offset=0, num_keys=len(data), endianness=0, format_flags=format_flags)
    
    # Serialize schema
    schema_bytes = serialize_schema(schema)
    header.schema_size = len(schema_bytes)
    
    # Ensure all keys are bytes
    byte_keys_data = {k if isinstance(k, bytes) else PyUnicode_AsUTF8String(k): v for k, v in data.items()}

    # Sort keys
    sorted_keys = sorted(byte_keys_data.keys())

    # Create Bloom Filter
    bloom_filter = create_bloom_filter(sorted_keys)
    
    # Collect serialized key block and data block
    key_entries = []
    data_entries = []
    current_data_offset = 0
    
    for key in sorted_keys:
        value = byte_keys_data[key]

        key_bytes = key if isinstance(key, bytes) else key.encode('utf-8')
        
        # Create Data Entry
        data_entry = DataEntry(ormsgpack.packb(value, option=ormsgpack.OPT_SERIALIZE_NUMPY, default=default_serializer))
        data_entries.append(data_entry)
        
        # Create Key Entry
        key_entry = KeyEntry(key_bytes, current_data_offset)
        key_entries.append(key_entry)
        
        # Update data offset
        current_data_offset += len(data_entry.serialize())
    
    # Serialize blocks
    raw_bloom_filter_bytes = bloom_filter.serialize()
    raw_key_block_bytes = b''.join([entry.serialize() for entry in key_entries])
    raw_data_block_bytes = b''.join([entry.serialize() for entry in data_entries])

    # compress the blocks
    bloom_filter_bytes = zstd.compress(bytes(raw_bloom_filter_bytes))
    key_block_bytes = zstd.compress(raw_key_block_bytes)
    data_block_bytes = zstd.compress(raw_data_block_bytes)

    print("pre", len(raw_key_block_bytes), "post", len(key_block_bytes))
    print("pre", len(raw_data_block_bytes), "post", len(data_block_bytes))

    # Set offsets in header
    header.schema_offset = len(header.serialize())
    header.bloom_filter_offset = header.schema_offset + len(schema_bytes)
    header.key_block_offset = header.bloom_filter_offset + len(bloom_filter_bytes)
    header.data_block_offset = header.key_block_offset + len(key_block_bytes)
    
    # Compute checksum for the header
    header.compute_checksum(raw_key_block_bytes, raw_data_block_bytes)
    
    # Construct SSTable
    sstable_bytes = header.serialize() + schema_bytes + bloom_filter_bytes + key_block_bytes + data_block_bytes
    return sstable_bytes



cpdef list load_sst(bytes sstable):
    """
    Parameters:
    sstable (bytes): The serialized SSTable containing the header, schema, 
                     Bloom filter, key block, and compressed data block.

    Returns:
    list: The decompressed data block entries.
    """
    # Deserialize header
    cdef Header header
    cdef bytes header_data = sstable[:HEADER_SIZE]
    header = Header.deserialize(header_data)
    
    # Locate the blocks using memoryview for efficiency
    cdef uint32_t key_block_start = header.key_block_offset
    cdef uint32_t key_block_end = header.data_block_offset
    cdef bytes compressed_key_block = sstable[key_block_start:key_block_end]
    cdef memoryview key_block = memoryview(zstd.decompress(compressed_key_block))

    cdef uint32_t data_block_start = header.data_block_offset
    cdef bytes compressed_data_block = sstable[data_block_start:]
    cdef memoryview data_block = memoryview(zstd.decompress(compressed_data_block))
    
    cdef memoryview key_entry_data
    cdef KeyEntry key_entry
    cdef DataEntry data_entry
    cdef int offset = 0
    cdef int data_offset
    cdef list values = [dict()] * header.num_keys
    cdef int index = 0

    while offset < key_block.shape[0]:
        key_entry_data = key_block[offset:offset + KEY_ENTRY_SIZE]
        key_entry = KeyEntry.deserialize(key_entry_data)

        data_offset = key_entry.data_block_offset
        data_entry_data = data_block[data_offset:]
        data_entry = DataEntry.deserialize(data_entry_data)

        values[index] = ormsgpack.unpackb(data_entry.data)
        index += 1

        offset += KEY_ENTRY_SIZE

    return values



cpdef list match_equals(bytes sstable, object value):
    """
    Searches for the exact match of a value in the SSTable.

    This function first utilizes a Bloom filter to quickly check if the given 
    value might be present in the SSTable, which reduces unnecessary binary 
    search operations. If the Bloom filter indicates the value might be present, 
    it performs a binary search on the decompressed key block to locate the 
    key. If the key is found, the corresponding data entry is retrieved from 
    the decompressed data block and deserialized.

    Parameters:
    sstable (bytes): The serialized SSTable containing the header, schema, 
                     Bloom filter, key block, and compressed data block.
    value (bytes):   The key to search for in the SSTable.

    Returns:
    list: A list containing the deserialized value associated with the given 
          key if found. Returns an empty list if the key is not found.
    """
    value = value[:64]
    value = value if isinstance(value, bytes) else PyUnicode_AsUTF8String(value)

    # Deserialize header
    cdef Header header
    header_data = sstable[:HEADER_SIZE]
    header = Header.deserialize(header_data)
    
    # Deserialize bloom filter
    bloom_filter_start = header.bloom_filter_offset
    bloom_filter_end = header.key_block_offset
    compressed_bloom_filter = sstable[bloom_filter_start:bloom_filter_end]
    bloom_filter_data = zstd.decompress(compressed_bloom_filter)
    bloom_filter = deserialize_bloom_filter(bloom_filter_data)
    
    # Check bloom filter membership
    if not bloom_filter.possibly_contains(value):
        # If the bloom filter indicates the value is not present, return an empty list
        print("not in bloom")
        return []

    # Locate the blocks
    cdef uint32_t key_block_start = header.key_block_offset
    cdef uint32_t key_block_end = header.data_block_offset
    cdef bytes compressed_key_block = sstable[key_block_start:key_block_end]
    cdef bytes key_block = zstd.decompress(compressed_key_block)

    # Perform binary search on key block
    cdef uint32_t low = 0
    cdef uint32_t high = len(key_block) // KEY_ENTRY_SIZE - 1
    cdef uint32_t mid, mid_offset
    cdef bytes key_entry_data

    cdef uint32_t data_block_start = header.data_block_offset
    cdef bytes compressed_data_block = sstable[data_block_start:]
    cdef bytes data_block = zstd.decompress(compressed_data_block)
    cdef bytes data_entry_data
    
    while low <= high:
        mid = (low + high) // 2
        mid_offset = mid * KEY_ENTRY_SIZE
        key_entry_data = key_block[mid_offset:mid_offset + KEY_ENTRY_SIZE]
        key_entry = KeyEntry.deserialize(key_entry_data)
        if key_entry.key == value:
            # Key found, retrieve data from data block
            data_offset = key_entry.data_block_offset
            data_entry_data = data_block[data_offset:]
            data_entry = DataEntry.deserialize(data_entry_data)
            return ormsgpack.unpackb(data_entry.data)
        elif key_entry.key < value:
            low = mid + 1
        else:
            high = mid - 1
    
    # Key not found
    return []