# cython: language_level=3

# Import necessary modules
from libc.stdint cimport uint8_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t

from cpython.mem cimport PyMem_Malloc, PyMem_Free
import struct
import zlib
import time
import zstd
import ormsgpack
from hashlib import sha256
from libc.stdlib cimport malloc, free
from libc.string cimport memset
from .bloom_filter cimport create_bloom_filter
from .bloom_filter cimport deserialize as deserialize_bloom_filter

cdef class Header:
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

    HEADER_SIZE = 42  # Updated size of the header in bytes

    def __init__(self, version: uint32_t, creation_timestamp: uint64_t, schema_offset: uint32_t,
                 schema_size: uint32_t, bloom_filter_offset: uint32_t, key_block_offset: uint32_t,
                 data_block_offset: uint32_t, num_keys: uint32_t, endianness: uint8_t, format_flags: uint8_t):
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

    def serialize(self) -> bytes:
        return struct.pack('<I Q I I I I I I B B I',
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
        (header.version, header.creation_timestamp, header.schema_offset, header.schema_size,
         header.bloom_filter_offset, header.key_block_offset, header.data_block_offset,
         header.num_keys, header.endianness, header.format_flags, header.checksum) = struct.unpack('<I Q I I I I I I B B I', data)
        return header

    def compute_checksum(self):
        self.checksum = zlib.crc32(self.serialize()[:-4]) & 0xffffffff

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
    def deserialize(data: bytes):
        key_length, key, data_block_offset = struct.unpack('<I64sI', data)
        return KeyEntry(key.rstrip(b'\x00'), data_block_offset)


cdef class DataEntry:
    cdef uint32_t data_length
    cdef public bytes data

    def __init__(self, data: bytes):
        self.data = data
        self.data_length = len(self.data)

    def serialize(self) -> bytes:
        return struct.pack('<I', self.data_length) + self.data

    @staticmethod
    def deserialize(data: bytes):
        data_length, = struct.unpack('<I', data[:4])
        raw_data = data[4:4 + data_length]
        return DataEntry(raw_data)


def serialize_schema(schema: dict) -> bytes:
    import orjson
    schema_json = orjson.dumps(schema)
    return schema_json

def deserialize_schema(schema_bytes: bytes) -> dict:
    import orjson
    return orjson.loads(schema_bytes)

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
    byte_keys_data = {k if isinstance(k, bytes) else k.encode('utf-8'): v for k, v in data.items()}

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
        data_entry = DataEntry(ormsgpack.packb(value))
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

    print("pre", len(raw_bloom_filter_bytes), "post", len(bloom_filter_bytes))
    print("pre", len(raw_key_block_bytes), "post", len(key_block_bytes))
    print("pre", len(raw_data_block_bytes), "post", len(data_block_bytes))

    # Set offsets in header
    header.schema_offset = len(header.serialize())
    header.bloom_filter_offset = header.schema_offset + len(schema_bytes)
    header.key_block_offset = header.bloom_filter_offset + len(bloom_filter_bytes)
    header.data_block_offset = header.key_block_offset + len(key_block_bytes)
    
    # Compute checksum for the header
    header.compute_checksum()
    
    # Construct SSTable
    sstable_bytes = header.serialize() + schema_bytes + bloom_filter_bytes + key_block_bytes + data_block_bytes
    return sstable_bytes



def match_equals(sstable: bytes, value: bytes) -> list:
    # Deserialize header
    header_size = 42  # Size of the header in bytes
    header_data = sstable[:header_size]
    header = Header.deserialize(header_data)
    
    # Locate key block and data block
    key_block_start = header.key_block_offset
    key_block_end = header.data_block_offset
    compressed_key_block = sstable[key_block_start:key_block_end]
    key_block = zstd.decompress(compressed_key_block)

    # Perform binary search on key block
    low, high = 0, len(key_block) // 72 - 1  # 72 bytes per key entry
    
    while low <= high:
        mid = (low + high) // 2
        mid_offset = mid * 72
        key_entry_data = key_block[mid_offset:mid_offset + 72]
        key_entry = KeyEntry.deserialize(key_entry_data)
        print(key_entry.key)
        if key_entry.key == value:
            # Key found, retrieve data from data block
            data_block_start = header.data_block_offset
            compressed_data_block = sstable[data_block_start:]
            data_block = zstd.decompress(compressed_data_block)

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
