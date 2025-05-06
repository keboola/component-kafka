#!/bin/sh

set -e

#find . -type f | sort

flake8 --config=flake8.cfg
echo "Running tests for main component..."
python -m unittest discover