import os
import sys

from orso.cityhash import CityHash32
from orso.tools import monitor
from orso.tools import random_string

from draken.compiled import murmurhash3

sys.path.insert(1, os.path.join(sys.path[0], ".."))


strings = [random_string(2048) for i in range(1_000_000)]


@monitor()
def do_city():
    [CityHash32(s) for s in strings]


@monitor()
def do_mur():
    [murmurhash3(s) for s in strings]


print("city")
do_city()
print("mur")
do_mur()
