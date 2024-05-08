from setuptools import find_packages, setup

with open("draken/version.py", "r") as v:
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
    name="draken",
    version=__version__,
    description="External Index",
    long_description=long_description,
    long_description_content_type="text/markdown",
    maintainer="Joocer",
    author="joocer",
    author_email="justin.joyce@joocer.com",
    packages=find_packages(include=["draken", "draken.*"]),
    url="https://github.com/mabel-dev/draken/",
    install_requires=required,
)
