"""
Setup script.

Use this script to install the ZeroMQ reader/writer incremental modules for the retico framework.
Usage:
    $ python3 setup.py install
The run the simulation:
    $ retico [-h]
"""

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

exec(open("retico_zmq/version.py").read())


import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "README.md").read_text(encoding="utf-8")

config = {
    "description": "The ZeroMQ reader/writer incremental modules for the retico framework",
    "long_description": long_description,
    "author": "Casey Kennington",
    "url": "https://github.com/retico-team/retico-zmq",
    "download_url": "https://github.com/retico-team/retico-zmq",
    "author_email": "caseykennington@boisestate.edu",
    "version": __version__,
    "packages": find_packages(),
    "name": "retico-zmq",
}

setup(**config)
