name: Refresh PAT Tokens

on:
  schedule:
    - cron: "0 1 * * *"  # every day at 01:00 UTC
  workflow_dispatch:

jobs:
  refresh-pat:
    runs-on: ubuntu-latest

    env:
      SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
      SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
      SNOWFLAKE_DATABASE: ${{ secrets.SNOWFLAKE_DATABASE }}
      SNOWFLAKE_SCHEMA: ${{ secrets.SNOWFLAKE_SCHEMA }}
      SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}

    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install snowflake-connector-python

      - name: Run refresh script
        run: python refresh_pat_data.py
