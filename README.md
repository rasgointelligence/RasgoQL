<p align="left">
  <img width="100%" href="https://rasgoml.com" target="_blank" src="https://files.gitbook.com/v0/b/gitbook-x-prod.appspot.com/o/spaces%2F-MJDKltt3A57jhixTfmu%2Fuploads%2F4noR7wUu8mqIv8k8v1Uk%2Fimage%20(20).png?alt=media&token=47b0b328-4585-44d7-9230-54b4f391b9e6" />
</p>

[![Downloads](https://pepy.tech/badge/rasgoql/month)](https://pepy.tech/project/rasgoql)
[![PyPI version](https://badge.fury.io/py/rasgoql.svg)](https://badge.fury.io/py/rasgoql)
[![Docs](https://img.shields.io/badge/RasgoQL-DOCS-GREEN.svg)](https://docs.rasgoql.com/)
[![Chat on Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://join.slack.com/t/rasgousergroup/shared_invite/zt-nytkq6np-ANEJvbUSbT2Gkvc8JICp3g)

# RasgoQL

RasgoQL is a Python package that enables you to easily query and transform tables in your Data Warehouse directly from a notebook. 

You can quickly create new features, sample data, apply complex aggregates... all without having to write SQL! 

Choose from our library of predefined transformations or make your own to streamline the feature engineering process.


# Why is this package useful?
Data scientists spend much of their time in pandas preparing data for modelling. When they are ready to deploy or scale, two pain points arise:
1. pandas cannot handle larger volumes of data, forcing the use of VMs or code refactoring.
2. feature data must be added to the Enterprise Data Warehouse for future processing, requiring refactoring to SQL

We created RasgoQL to solve these two pain points.

Learn more at [https://docs.rasgoql.com](https://docs.rasgoql.com). 

# How does it work?
Under the covers, RasgoQL sends all processing to your Data Warehouse, enabling the efficient transformation of massive datasets. RasgoQL only needs basic metadata to execute transforms, so your private data remains secure.

![RasgoQL workflow diagram](https://f.hubspotusercontent30.net/hubfs/20517936/rasgoql/RasgoQL_Flow.png)

RasgoQL does these things well:
- Pulls existing Data Warehouse tables into pandas DataFrames for analysis
- Constructs SQL queries using a syntax that feels like pandas
- Creates views in your Data Warehouse to save transformed data
- Exports runnable sql in .sql files or dbt-compliant .yaml files
- Offers dozens of free SQL transforms to use
- Coming Soon: allows users to create & add custom transforms

RasgoQL’s initial release will support connecting to your existing Snowflake Data Warehouse. We plan to add support for BigQuery and Postgres in the very near future. If you'd like to suggest another database type, submit your idea to our [GitHub Discussions page](https://github.com/rasgointelligence/RasgoQL/discussions) so that other community members can weight in and show their support. 

# Can RasgoQL help you?

* If you use pandas to build features, but you are working on a massive set of data that won't fit in your machine's memory. RasgoQL can help!

* If your organization uses dbt or another SQL tool to run production data flows, but you prefer to build features in pandas. RasgoQL can help!

* If you know pandas, but not SQL and want to learn how queries will translate. RasgoQL can help!

# Where to get it
Just run a simple pip install.

`pip install rasgoql`


# Quick Start
```python
pip install rasgoql --upgrade

# Connect to your data warehouse
creds = rasgoql.SnowflakeCredentials(
    account="",
    user="",
    password="",
    role="",
    warehouse="",
    database="",
    schema=""
)
rql = rasgoql.connect(creds)
print("rasgoQL connected!")

# Allow rasgoQL to interact with an existing Table in your Data Warehouse
ds = rql.dataset(fqtn='RASGOLOCAL.PUBLIC.ABCD123')
ds.preview()

# Create a SQL Chain by applying a Rasgo Transform
chn = tbl.cast(
    casts={
      'LOC_ID':'STRING',
      'DATE':'STRING'
    }
)

# Print the SQL
chn.sql()

```

# Advanced Examples

## Joins
Easily join tables together using the `join` transform. 

```python
internet_sales = rasgoql.dataset('ADVENTUREWORKS.PUBLIC.INTERNET_SALES')

ds_join = internet_sales.join(
  join_table='DIM_PRODUCT',
  join_columns={'PRODUCTKEY': 'PRODUCTKEY'},
  join_type='LEFT',
  join_prefix='PRODUCT')

ds_join.sql()
ds_join.preview()
```

## Chain transforms together
Create a rolling average aggregation and then drops unnecessary colomns. 

```python
ds_agg = ds.rolling_agg(
    aggregations={"SALESAMOUNT": ["MAX", "MIN", "SUM"]},
    order_by="ORDERDATE",
    offsets=[-7, 7],
    group_by=["PRODUCTKEY"],
).drop_columns(exclude_cols=["ORDERDATEKEY"])

ds_agg.sql()
ds_agg.preview()
```

## Transpose unique values with pivots 
Quickly generate pivot tables of your data.

```python
ds_pivot = ds_agg.pivot(
    dimensions=['ORDERDATE'],
    pivot_column='SALESAMOUNT',
    value_column='PRODUCTKEY',
    agg_method='SUM',
    list_of_vals=['310', '345']
)

ds_pivot.sql()
ds_pivot.preview()
```

# Where do I go for help?
If you have any questions please: 

1. [RasgoQL Docs](https://docs.rasgoql.com/)
2. [Slack](https://join.slack.com/t/rasgousergroup/shared_invite/zt-nytkq6np-ANEJvbUSbT2Gkvc8JICp3g)
3. [GitHub Issues](https://github.com/rasgointelligence/RasgoQL/issues)


# How can I contribute? 
Review the [contributors guide](https://github.com/rasgointelligence/RasgoQL/blob/main/CONTRIBUTING.md)


<i>Built for Data Scientists, by Data Scientists</i>

This project is sponspored by RasgoML. Find out at [https://www.rasgoml.com/](https://www.rasgoml.com/)