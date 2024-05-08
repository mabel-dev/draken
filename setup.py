from setuptools import find_packages, setup

LIBRARY = "draken"

__author__ = "notset"
__version__ = "notset"
with open(f"{LIBRARY}/__version__.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec

with open("README.md", "r") as rm:
    long_description = rm.read()

try:
    with open("requirements.txt", "r") as f:
        required = f.read().splitlines()
except:
    with open("draken.egg-info/requires.txt", "r") as f:
        required = f.read().splitlines()

setup(
    name=LIBRARY,
    version=__version__,
    description="External Index",
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer="@joocer",
    author=__author__,
    author_email="justin.joyce@joocer.com",
    packages=find_packages(include=[LIBRARY, f"{LIBRARY}.*"]),
    url=f"https://github.com/mabel-dev/{LIBRARY}/",
    install_requires=required,
)
