import platform

from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup
import numpy

LIBRARY = "draken"

__author__ = "notset"
__version__ = "0.0.0"
with open(f"{LIBRARY}/__version__.py", mode="r") as v:
    vers = v.read()
# xec(vers)  # nosec


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


if is_mac():
    COMPILE_FLAGS = ["-O2"]
else:
    COMPILE_FLAGS = ["-O2", "-march=native"]


with open("README.md", "r") as rm:
    long_description = rm.read()

try:
    with open("requirements.txt", "r") as f:
        required = f.read().splitlines()
except:
    with open("draken.egg-info/requires.txt", "r") as f:
        required = f.read().splitlines()

extensions = [
    Extension(
        name="draken.compiled.murmurhash3_32",
        sources=["draken/compiled/murmurhash3_32.pyx"],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="draken.compiled.bloom_filter",
        sources=["draken/compiled/bloom_filter.pyx"],
        include_dirs=[numpy.get_include()],
        extra_compile_args=COMPILE_FLAGS,
    ),
    Extension(
        name="draken.compiled.sstable",
        sources=["draken/compiled/sstable.pyx"],
        extra_compile_args=COMPILE_FLAGS,
        include_dirs=[".", numpy.get_include()],
    ),
    Extension(
        name="draken.compiled.accumulation_tree",
        sources=["draken/compiled/accumulation_tree.pyx"],
        extra_compile_args=COMPILE_FLAGS,
        include_dirs=["."],
    ),
]

setup_config = {
    "name": LIBRARY,
    "version": __version__,
    "description": "Draken - External Indexes",
    "long_description": long_description,
    "long_description_content_type": "text/markdown",
    "maintainer": "@joocer",
    "author": __author__,
    "author_email": "justin.joyce@joocer.com",
    "packages": find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    "python_requires": ">=3.9",
    "url": "https://github.com/mabel-dev/{LIBRARY}/",
    "install_requires": required,
    "ext_modules": cythonize(extensions),
    "package_data": {
        "": ["*.pyx", "*.pxd"],
    },
}

setup(**setup_config)
