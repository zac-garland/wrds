#!/usr/bin/env bash
set -euo pipefail

python -m pip install --upgrade pip
python -m pip install pre-commit
pre-commit install
echo "pre-commit installed. Next: try 'pre-commit run --all-files'."

