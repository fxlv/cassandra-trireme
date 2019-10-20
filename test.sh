#!/usr/bin/env bash
echo "Running pylint"
pylint count.py
echo "Running pep8"
pycodestyle count.py
echo "Running tests"
pytest -v --durations=6 --cov-report=term-missing --cov=.
