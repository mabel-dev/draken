import os
import random
import sys
import time

from orso.tools import random_string

from draken.compiled import StringBinaryIndex

sys.path.insert(1, os.path.join(sys.path[0], ".."))


# Performance test harness
def performance_test():
    index = StringBinaryIndex()

    # Generate 10 million items for about 50,000 keys
    num_keys = 50000
    num_items = 10000000

    keys = [random_string(10) for _ in range(num_keys)]
    filenames = [random_string(10) for _ in range(num_items)]

    start_time = time.time()
    for i in range(num_items):
        key = random.choice(keys)
        filename = filenames[i]
        offset = random.randint(0, 1000000)
        index.add_entry(key, filename, offset)

    print(f"Time to add entries: {time.time() - start_time:.4f} seconds")

    start_time = time.time()
    index.finalize_index()
    print(f"Time to finalize index: {time.time() - start_time:.4f} seconds")

    # Perform lookup_eq
    lookup_key = random.choice(keys)
    start_time = time.monotonic_ns()
    result_eq = index.lookup_eq(lookup_key)
    print(f"Time for lookup_eq: {(time.monotonic_ns() - start_time)/1e6:.4f} milliseconds")
    print(f"Results for lookup_eq: {len(result_eq)} items")

    # Perform lookup_in_list
    lookup_keys = random.sample(keys, 100)
    start_time = time.monotonic_ns()
    result_in_list = index.lookup_in_list(lookup_keys)
    print(f"Time for lookup_in_list: {(time.monotonic_ns() - start_time)/1e6:.4f} milliseconds")
    print(f"Results for lookup_in_list: {sum(len(v) for v in result_in_list.values())} items")

    # Perform lookup_range
    start_key = random.choice(keys)
    end_key = random.choice(keys)
    start_key, end_key = min(start_key, end_key), max(start_key, end_key)
    start_time = time.time()
    result_range = index.lookup_range(start_key, end_key)
    print(f"Time for lookup_range: {time.time() - start_time:.4f} seconds")
    print(f"Results for lookup_range: {sum(len(v) for v in result_range.values())} items")


if __name__ == "__main__":
    performance_test()
