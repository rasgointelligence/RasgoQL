#!/bin/bash

set -e

if [ 1 != $# ]; then
    echo "usage: $0 index"
    echo "Index values: pypi pypitest"
    exit 1;
fi
PYPI_INDEX="$1"

# Remove old build artifacts
rm -rf dist/*

# Generate new artifacts
python setup.py sdist bdist_wheel

# Upload artifacts to pypi
python -m twine upload --verbose  -r "$PYPI_INDEX" dist/*
