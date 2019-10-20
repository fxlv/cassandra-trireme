#!/usr/bin/env bash
echo "Running pylint"
pylint count.py
echp "Running pep8"
pep8 count.py
echo "Running tests"
pytest -v --durations=6 --cov-report=term-missing --cov=.