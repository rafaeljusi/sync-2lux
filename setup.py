#!/usr/bin/env python
from setuptools import setup

setup(
    name="sync-2lux",
    version="0.1.0",
    description="Script for extracting data",
    author="Rafael Jusi",
    url="http://www.jusi.com.br",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["sync_2lux"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "pandas",
    ],
    entry_points="""
    [console_scripts]
    sync-2lux=sync_2lux:main
    """,
    packages=["sync_2lux"],
    package_data = {
        "schemas": ["sync_2lux/schemas/*.json"]
    },
    include_package_data=True,
)
