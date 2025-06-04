# Codex agent instructions

## How to run tests
pytest -q

## Lint / format
ruff .
black --check .

## Execution entry-points
* Historical load → src/lcr_etl/historical.py
* Delta/append load → src/lcr_etl/delta.py

## Coding standards
* PEP 484 types everywhere
* Black 88-char lines, single-line comments only
* Use Spark 3.5 typed DataFrames & ACC APIs
* Target Python 3.12

## QA alignment
See docs/qa_observations.md for the data-quality mismatches Codex must fix.
