"""Build counter utility for the Draken project.

This script increments the build number in the __version__.py file and commits
the change to version control. It reads the current build number, increments it,
updates the version file, and stages the changes for commit.
"""
import subprocess

__build__ = None

with open("draken/__version__.py", "r") as f:
    contents = f.read().splitlines()[0]

__build__ = contents.split("=")[-1].strip().replace("'", "").replace('"', "")

if __build__:
    __build__ = int(__build__) + 1

    with open("draken/__version__.py", "r") as f:
        contents = f.read().splitlines()[1:]

    # Save the build number to the build.py file
    with open("draken/__version__.py", "w") as f:
        f.write(f"__build__ = {__build__}\n")
        f.write("\n".join(contents) + "\n")

__version__ = "notset"
with open("draken/__version__.py", mode="r") as v:
    vers = v.read()
exec(vers)  # nosec
print(__version__)

subprocess.run(["git", "add", "draken/__version__.py"])
