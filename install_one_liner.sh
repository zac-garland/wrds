#!/bin/bash
# One-liner: python3 -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install pandas numpy sqlalchemy psycopg2-binary scipy statsmodels plotly plotly-express wrds
python3 -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install pandas numpy sqlalchemy psycopg2-binary scipy statsmodels plotly plotly-express wrds && echo "✓ Installation complete! Activate with: source venv/bin/activate"
