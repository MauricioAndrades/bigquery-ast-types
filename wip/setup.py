"""
Setup configuration for bigquery-ast-types
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="bigquery-ast-types",
    version="0.1.0",
    author="Little Bow Wow",
    description="A comprehensive AST library for BigQuery SQL manipulation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/bigquery-ast-types",
    packages=find_packages(exclude=["tests", "examples", "utils"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "sqlglot>=25.0.0",
    ],
    extras_require={
        "tests": [
            "pytest>=7.0",
            "pytz",
            "duckdb",
            "numpy",
            "pandas",
        ],
        "examples": [
            "pendulum",
        ],
        "dev": [
            "pytest>=7.0",
            "black>=22.0",
            "mypy>=1.0",
            "ruff>=0.0.261",
            "pytz",
            "duckdb",
            "numpy",
            "pandas",
            "pendulum",
        ],
    },
)
