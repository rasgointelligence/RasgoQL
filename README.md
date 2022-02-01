<p align="left">
  <img width="100%" href="https://rasgoml.com" target="_blank" src="https://files.gitbook.com/v0/b/gitbook-x-prod.appspot.com/o/spaces%2F-MJDKltt3A57jhixTfmu%2Fuploads%2F4noR7wUu8mqIv8k8v1Uk%2Fimage%20(20).png?alt=media&token=47b0b328-4585-44d7-9230-54b4f391b9e6" />
</p>

[![PyPI version](https://badge.fury.io/py/pyrasgo.svg)](https://badge.fury.io/py/pyrasgo)
[![Docs](https://img.shields.io/badge/PyRasgo-DOCS-GREEN.svg)](https://docs.rasgoml.com/)
[![Chat on Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://join.slack.com/t/rasgousergroup/shared_invite/zt-nytkq6np-ANEJvbUSbT2Gkvc8JICp3g)
[![Chat on Discourse](https://img.shields.io/discourse/status?server=https%3A%2F%2Fforum.rasgoml.com)](https://forum.rasgoml.com/)

# RasgoQL

RasgoQL is a light-weight data transformation package to bridge the gap between dbt and pandas. It allow users to construct, print & run SQL queries using a familiar python syntax. Under the covers it sends all processing to your data warehouse, allowing efficient transformation of massive datasets.

RasgoQL does these things well:
- Pulls existing DataWarehouse tables into pandas DataFrames for analysis
- Constructs SQL queries using a syntax that feels like pandas
- Creates views in your DataWarehouse to save transformed data
- Exports runnable sql in .sql files a dbt-compliant yml files
- Offers dozens of free SQL transforms to use
- Coming Soon: allows users to create & add custom transforms

# Quick Start
```python
pip install rasgoql --upgrade

# Connect to your data warehouse
# DW Creds helper class
creds = rasgoql.SnowflakeCredentials(
    account="",
    user="",
    password="",
    role="",
    warehouse="",
    database="",
    schema=""
)
print(creds)

# Main Connection workflow
rql = rasgoql.connect(dw='snowflake', credentials=creds)
print("rasgoQL connected!")

# Allow rasgoQL to interact with an existing Table in your Data Warehouse
tbl = rql.table(fqtn='RASGOLOCAL.PUBLIC.ABCD123')
tbl.preview()

# Create a Chain by applying Transform(s)
chn = tbl.cast(
    casts={
      'LOC_ID':'STRING',
      'DATE':'STRING'
    }
)
chn

# Print SQL
print(chn.sql())

```

# Can RasgoQL help you?

If you use pandas to build features, but you are working on a massive set of data that won't fit in your machine's memory. RasgoQL can help!

If your organization uses dbt of another SQL tool to run production data flows, but you prefer to build features in pandas. RasgoQL can help!

If you know pandas, but not SQL and want to learn how queries will translate. RasgoQL can help!

<i>Built for Data Scientists, by Data Scientists</i>
