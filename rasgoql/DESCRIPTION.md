<p align="left">
  <img width="90%" href="https://rasgoml.com" target="_blank" src="https://f.hubspotusercontent30.net/hubfs/20517936/rasgoql/Rasgo_Logo_WBackground.png" />
</p>

# RasgoQL
RasgoQL is a light-weight data transformation package to bridge the gap between dbt and pandas. It allow users to construct, print & run SQL queries using a familiar python syntax. Under the covers it sends all processing to your data warehouse, allowing efficient transformation of massive datasets.

RasgoQL does these things well:

- Pulls existing DataWarehouse tables into pandas DataFrames for analysis
- Constructs SQL queries using a syntax that feels like pandas
- Creates views in your DataWarehouse to save transformed data
- Exports runnable sql in .sql files or dbt-compliant yml files
- Offers dozens of free SQL transforms to use
- Coming Soon: allows users to create & add custom transforms

Documentation is available at:
https://docs.rasgoql.com

## Ways RasgoQL can help

* If you use pandas to build features, but you are working on a massive set of data that won't fit in your machine's memory. RasgoQL can help!

* If your organization uses dbt of another SQL tool to run production data flows, but you prefer to build features in pandas. RasgoQL can help!

* If you know pandas, but not SQL and want to learn how queries will translate. RasgoQL can help!


## Package Dependencies
-------------------------------------------------------------------------------
- jinja2
- pandas
- pyyaml
- python-dotenv
- rasgotransforms

[snowflake]
- snowflake-connector-python
- snowflake-connector-python[pandas]

[bigquery]
- google-auth-oauthlib
- google-cloud-bigquery


## Major Version Releases
-------------------------------------------------------------------------------
- v1.0.0 (Feb 23, 2022)
   - Added support for BigQuery
   - Added support for Snowflake
   - Added import dataset from pandas workflow
   - Added export to dbt workflow

- v1.1.0 (Mar 8, 2022)
    - Added support for Postgres


See [Changelog](https://github.com/rasgointelligence/RasgoQL/blob/main/rasgoql/CHANGELOG.md) for full minor version release notes

## About Us
RasgoQL is maintained by *[Rasgo](https://rasgoml.com)*. Rasgo's enterprise feature store integrates with your data warehouse to help users build features faster, collaborate with team members, and serve features to models in production.


<i>Built for Data Scientists, by Data Scientists</i>
