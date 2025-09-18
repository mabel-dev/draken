import os
import platform
from sysconfig import get_config_var

from Cython.Build import cythonize
from setuptools import Extension
from setuptools import find_packages
from setuptools import setup


def is_mac():  # pragma: no cover
    return platform.system().lower() == "darwin"


def is_win():  # pragma: no cover
    return platform.system().lower() == "windows"


LIBRARY = "draken"
CPP_COMPILE_FLAGS = ["-O3"]
C_COMPILE_FLAGS = ["-O3"]
if is_mac():
    CPP_COMPILE_FLAGS += ["-std=c++17"]
elif is_win():
    CPP_COMPILE_FLAGS += ["/std:c++17"]
else:
    CPP_COMPILE_FLAGS += ["-std=c++17", "-march=native", "-fvisibility=default"]
    C_COMPILE_FLAGS += ["-march=native", "-fvisibility=default"]

include_dirs = []
# Get the C++ include directory
includedir = get_config_var("INCLUDEDIR")
if includedir:
    include_dirs.append(os.path.join(includedir, "c++", "v1"))

# Get the Python include directory
includepy = get_config_var("INCLUDEPY")
if includepy:
    include_dirs.append(includepy)

# Check if paths exist
include_dirs = [p for p in include_dirs if os.path.exists(p)]

print("\033[38;2;255;85;85mInclude paths:\033[0m", include_dirs)


__author__ = "notset"
__version__ = "notset"
_status = None
VersionStatus = None
with open(f"{LIBRARY}/__version__.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec

RELEASE_CANDIDATE = _status == VersionStatus.RELEASE
COMPILER_DIRECTIVES = {"language_level": "3"}
COMPILER_DIRECTIVES["profile"] = not RELEASE_CANDIDATE
COMPILER_DIRECTIVES["linetrace"] = not RELEASE_CANDIDATE

print(f"\033[38;2;255;85;85mBuilding Draken version:\033[0m {__version__}")
print(f"\033[38;2;255;85;85mStatus:\033[0m {_status}", "(rc)" if RELEASE_CANDIDATE else "")

with open("README.md", mode="r", encoding="UTF8") as rm:
    long_description = rm.read()

#try:
#    with open("requirements.txt", "r") as f:
#        required = f.read().splitlines()
#except:
#    with open(f"{LIBRARY}.egg-info/requires.txt", "r") as f:
#        required = f.read().splitlines()

extensions = [

    Extension(
        "draken.interop.arrow",
        sources=["draken/interop/arrow.pyx"],
        extra_compile_args=C_COMPILE_FLAGS,
        include_dirs=include_dirs + ["draken"],
        depends=[
            "draken/core/buffers.h",
            "draken/interop/arrow_c_data_interface.h"
        ],
    ),
    Extension(
        name="draken.vectors.vector",
        sources=["draken/vectors/vector.pyx"],
        extra_compile_args=C_COMPILE_FLAGS,
        include_dirs=include_dirs + ["draken"],
        depends=["draken/core/buffers.h"],
    ),
    Extension(
        name="draken.vectors.int64_vector",
        sources=["draken/vectors/int64_vector.pyx"],
        extra_compile_args=C_COMPILE_FLAGS,
        include_dirs=include_dirs + ["draken"],
        depends=["draken/core/buffers.h"],
    ),
    Extension(
        name="draken.vectors.string_vector",
        sources=["draken/vectors/string_vector.pyx"],
        extra_compile_args=C_COMPILE_FLAGS,
        include_dirs=include_dirs + ["draken"],
        depends=["draken/core/buffers.h"],
    ),
    Extension(
        name="draken.morsels.morsel",
        sources=["draken/morsels/morsel.pyx"],
        extra_compile_args=C_COMPILE_FLAGS,
        include_dirs=include_dirs + ["draken"],
        depends=[
            "draken/core/buffers.h"
            "draken/morsels/morsel.h"
        ],
    ),

]

# Add SIMD support flags
machine = platform.machine().lower()
system = platform.system().lower()
if machine.startswith("arm") and not machine.startswith("aarch64"):
    if system != "darwin":
        CPP_COMPILE_FLAGS.append("-mfpu=neon")
elif "x86" in machine or "amd64" in machine:
    CPP_COMPILE_FLAGS.append("-mavx2")

setup_config = {
    "name": LIBRARY,
    "version": __version__,
    "description": "Cython compatibility layer for Arrow",
    "long_description": long_description,
    "long_description_content_type": "text/markdown",
    "maintainer": "@joocer",
    "author": __author__,
    "author_email": "justin.joyce@joocer.com",
    "packages": find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    "python_requires": ">=3.9",
    "url": "https://github.com/mabel-dev/draken/",
#    "install_requires": required,
    "ext_modules": cythonize(extensions),
    "package_data": {
        "": ["*.pyx", "*.pxd"],
    },
    "compiler_directives": COMPILER_DIRECTIVES,
}

setup(**setup_config)
