
from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import datetime
import utils

setup(
    name="dlt_asset_bundle_skt",
    version=utils.__version__ + "+" + datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
    author="sathesh.subramanium@mccain.ca",
    description="test package",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={
        "packages": [
            "main=utils.main:main"
        ]
    },
    install_requires=[
        "setuptools"
    ],
)
