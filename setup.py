#!/usr/bin/env python3

"""
Setup script for Dremio Benchmark
"""

from setuptools import setup, find_packages
import os

# Read requirements from requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

# Read long description from README.md
with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="dremio-benchmark",
    version="1.0.0",
    description="Benchmarking framework for Dremio across multiple formats and clusters",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="RaoruNaren219",
    author_email="narendranraoru2219@gmail.com",
    url="https://github.com/RaoruNaren219/dremio-benchmark",
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "dremio-benchmark=main:main",
        ],
    },
) 