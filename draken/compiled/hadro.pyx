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

# Define Header class
cdef class Header:
    cdef public uint32_t version
    cdef public uint64_t creation_timestamp
    cdef public uint32_t bloom_filter_offset
    cdef public uint32_t term_dictionary_offset
    cdef public uint32_t postings_list_offset
    cdef public uint32_t data_block_offset
    cdef public uint32_t num_terms
    cdef public uint8_t endianness
    cdef public uint32_t checksum

    def __init__(self, version: uint32_t, creation_timestamp: uint64_t, bloom_filter_offset: uint32_t,
                 term_dictionary_offset: uint32_t, postings_list_offset: uint32_t, data_block_offset: uint3232_t,
                 num_terms: uint32_t, endianness: uint8_t):
        self.version = version
        self.creation_timestamp = creation_timestamp
        self.bloom_filter_offset = bloom_filter_offset
        self.term_dictionary_offset = term_dictionary_offset
        self.postings_list_offset = postings_list_offset
        self.data_block_offset = data_block_offset
        self.num_terms = num_terms
        self.endianness = endianness
        self.checksum = 0  # to be computed later

    def serialize(self) -> bytes:
        return struct.pack('<I Q I I I I I B I',
                           self.version,
                           self.creation_timestamp,
                           self.bloom_filter_offset,
                           self.term_dictionary_offset,
                           self.postings_list_offset,
                           self.data_block_offset,
                           self.num_terms,
                           self.endianness,
                           self.checksum)

    def compute_checksum(self):
        self.checksum = zlib.crc32(self.serialize()[:-4]) & 0xffffffff

    @staticmethod
    def deserialize(data: bytes):
        header = Header.__new__(Header)
        (header.version, header.creation_timestamp, header.bloom_filter_offset, header.term_dictionary_offset,
         header.postings_list_offset, header.data_block_offset, header.num_terms, header.endianness, header.checksum) = struct.unpack('<I Q I I I I I B I', data)
        return header

    @staticmethod
    def size() -> uint32_t:
        return 37

# Define BloomFilter class
cdef class BloomFilter:
    cdef uint32_t size
    cdef uint8_t* bit_array
    cdef list hash_functions

    def __init__(self, size: int, hash_functions: list):
        self.size = size
        self.hash_functions = hash_functions
        self.bit_array = <uint8_t *> malloc(size * sizeof(uint8_t))
        memset(self.bit_array, 0, size * sizeof(uint8_t))

    def add(self, key: bytes):
        for func in self.hash_functions:
            index = func(key) % self.size
            self.bit_array[index] = 1

    def might_contain(self, key: bytes) -> bool:
        for func in self.hash_functions:
            index = func(key) % self.size
            if self.bit_array[index] == 0:
                return False
        return True

    def serialize(self) -> bytes:
        return bytes(self.bit_array[:self.size])

    def __dealloc__(self):
        free(self.bit_array)

# Define TermEntry class
cdef class TermEntry:
    cdef uint32_t term_length
    cdef bytes term
    cdef uint32_t postings_offset

    def __init__(self, term: bytes, postings_offset: uint32_t):
        self.term_length = min(len(term), 64)  # cap term length to 64 characters
        self.term = term[:64]
        self.postings_offset = postings_offset

    def serialize(self) -> bytes:
        return struct.pack('<I64sI', self.term_length, self.term.ljust(64, b'\x00'), self.postings_offset)

# Define PostingsEntry class
cdef class PostingsEntry:
    cdef uint32_t doc_id
    cdef uint32_t frequency
    cdef list positions

    def __init__(self, doc_id: uint32_t, frequency: uint32_t, positions: list):
        self.doc_id = doc_id
        self.frequency = frequency
        self.positions = positions

    def serialize(self) -> bytes:
        pos_bytes = b''.join([struct.pack('<I', pos) for pos in self.positions])
        return struct.pack('<II', self.doc_id, self.frequency) + pos_bytes

# Define DataEntry class
cdef class DataEntry:
    cdef uint32_t data_length
    cdef bytes data

    def __init__(self, data: bytes):
        self.data = zstd.compress(data)
        self.data_length = len(self.data)

    def serialize(self) -> bytes:
        return struct.pack('<I', self.data_length) + self.data

# Define the SSTable creation function
def create_sstable(dict data) -> bytes:
    def hash_function_1(key: bytes) -> int:
        return int.from_bytes(sha256(key).digest(), byteorder='little')

    def hash_function_2(key: bytes) -> int:
        return int.from_bytes(sha256(key[::-1]).digest(), byteorder='little')

    # Define header
    creation_timestamp = int(time.time())
    header = Header(version=1, creation_timestamp=creation_timestamp, bloom_filter_offset=0,
                    term_dictionary_offset=0, postings_list_offset=0, data_block_offset=0,
                    num_terms=len(data), endianness=0)
    
    # Create Bloom Filter
    bloom_filter = BloomFilter(size=524288, hash_functions=[hash_function_1, hash_function_2])
    
    # Collect serialized term dictionary and postings list
    term_entries = []
    postings_entries = []
    current_postings_offset = 0
    current_data_offset = 0
    data_block_bytes = b''
    
    for term, postings in data.items():
        term_bytes = term.encode('utf-8')
        
        # Add to Bloom filter
        bloom_filter.add(term_bytes)
        
        # Create Postings Entry
        postings_entry = PostingsEntry(postings['doc_id'], postings['frequency'], postings['positions'])
        postings_entries.append(postings_entry)
        
        # Create Term Entry
        term_entry = TermEntry(term_bytes, current_postings_offset)
        term_entries.append(term_entry)
        
        # Update postings offset
        current_postings_offset += len(postings_entry.serialize())
        
        # Serialize data block
        value_bytes = ormsgpack.packb(postings)
        data_entry = DataEntry(value_bytes)
        data_block_bytes += data_entry.serialize()
        current_data_offset += len(data_entry.serialize())
    
    # Serialize blocks
    bloom_filter_bytes = bloom_filter.serialize()
    term_dictionary_bytes = b''.join([entry.serialize() for entry in term_entries])
    postings_list_bytes = b''.join([entry.serialize() for entry in postings_entries])
    
    # Set offsets in header
    header.bloom_filter_offset = len(header.serialize())
    header.term_dictionary_offset = header.bloom_filter_offset + len(bloom_filter_bytes)
    header.postings_list_offset = header.term_dictionary_offset + len(term_dictionary_bytes)
    header.data_block_offset = header.postings_list_offset + len(postings_list_bytes)
    
    # Compute checksum for the header
    header.compute_checksum()
    
    # Construct SSTable
    sstable_bytes = header.serialize() + bloom_filter_bytes + term_dictionary_bytes + postings_list_bytes + data_block_bytes
    return sstable_bytes

# Define the SSTable lookup functions
def lookup_eq(bytes sstable, str key) -> dict:
    header = Header.deserialize(sstable[:Header.size()])
    bloom_filter = BloomFilter.deserialize(sstable[header.bloom_filter_offset:header.term_dictionary_offset])
    
    key_bytes = key.encode('utf-8')
    
    if not bloom_filter.might_contain(key_bytes):
        return None
    
    # Binary search in term dictionary block
    term_dictionary = sstable[header.term_dictionary_offset:header.postings_list_offset]
    low, high = 0, header.num_terms - 1
    
    while low <= high:
        mid = (low + high) // 2
        mid_term_entry = TermEntry.deserialize(term_dictionary[mid * TermEntry.size():(mid + 1) * TermEntry.size()])
        
        if mid_term_entry.term == key_bytes:
            postings_offset = mid_term_entry.postings_offset
            postings_entry = PostingsEntry.deserialize(sstable[header.postings_list_offset + postings_offset:])
            return {
                'doc_id': postings_entry.doc_id,
                'frequency': postings_entry.frequency,
                'positions': postings_entry.positions
            }
        elif mid_term_entry.term < key_bytes:
            low = mid + 1
        else:
            high = mid - 1
    
    return None

def lookup_in_list(bytes sstable, list keys) -> dict:
    result = {}
    for key in keys:
        value = lookup_eq(sstable, key)
        if value is not None:
            result[key] = value
    return result

def lookup_range(bytes sstable, str key, str comparison) -> list:
    header = Header.deserialize(sstable[:Header.size()])
    term_dictionary = sstable[header.term_dictionary_offset:header.postings_list_offset]
    
    key_bytes = key.encode('utf-8')
    result = []
    
    if comparison == 'GT':
        for i in range(header.num_terms):
            entry = TermEntry.deserialize(term_dictionary[i * TermEntry.size():(i + 1) * TermEntry.size()])
            if entry.term > key_bytes:
                postings_offset = entry.postings_offset
                postings_entry = PostingsEntry.deserialize(sstable[header.postings_list_offset + postings_offset:])
                result.append({
                    'doc_id': postings_entry.doc_id,
                    'frequency': postings_entry.frequency,
                    'positions': postings_entry.positions
                })
    elif comparison == 'LT':
        for i in range(header.num_terms):
            entry = TermEntry.deserialize(term_dictionary[i * TermEntry.size():(i + 1) * TermEntry.size()])
            if entry.term < key_bytes:
                postings_offset = entry.postings_offset
                postings_entry = PostingsEntry.deserialize(sstable[header.postings_list_offset + postings_offset:])
                result.append({
                    'doc_id': postings_entry.doc_id,
                    'frequency': postings_entry.frequency,
                    'positions': postings_entry.positions
                })
    
    return result
