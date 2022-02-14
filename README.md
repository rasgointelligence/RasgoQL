<p align="left">
  <img width="100%" href="https://rasgoml.com" target="_blank" src="https://files.gitbook.com/v0/b/gitbook-x-prod.appspot.com/o/spaces%2F-MJDKltt3A57jhixTfmu%2Fuploads%2F4noR7wUu8mqIv8k8v1Uk%2Fimage%20(20).png?alt=media&token=47b0b328-4585-44d7-9230-54b4f391b9e6" />
</p>

[![Downloads](https://pepy.tech/badge/rasgoql/month)](https://pepy.tech/project/rasgoql)
[![PyPI version](https://badge.fury.io/py/rasgoql.svg)](https://badge.fury.io/py/rasgoql)
[![Docs](https://img.shields.io/badge/RasgoQL-DOCS-GREEN.svg)](https://docs.rasgoql.com/)
[![Chat on Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://join.slack.com/t/rasgousergroup/shared_invite/zt-nytkq6np-ANEJvbUSbT2Gkvc8JICp3g)

# RasgoQL

Transform data directly in python, no SQL required.

RasgoQL is a Python package that enables you to easily query and transform data housed in your database engine directly from your notebook or IDE of choice. Quickly create new features, filter and sample your data, change column types, create aggregates and so much more! All without having to write/wrangle SQL, or duplicate data to your local machine! 

Choose from our library of predefined transformations or make your own to streamline the process of extracting, transforming and loading large amounts of data.

[insert gif of RasgoQL transform in action]
<!-- Gif of RasgoQL transform -->


# Why is this project useful?
Data scientists spend most of their time cleaning and preparing data in Python only to then go back and have to rewrite everything into SQL. We created RasgoQL as a data transformation package that gives users the ability to transform large amounts of data directly within their notebook by creating SQL that runs in the database.

Learn more at [https://docs.rasgoql.com](https://docs.rasgoql.com). 

# How does it work?
Under the covers, it sends all processing to your data warehouse, enabling the efficient transformation of massive datasets without the duplication of data. Also RasgoQL only needs to basic metadata to execute transforms, so your private data remains secure and housed within the warehouse.

![RasgoQL workflow diagram](https://f.hubspotusercontent30.net/hubfs/20517936/rasgoql/RasgoQL_Flow.png)

RasgoQL does these things well:
- Pulls existing DataWarehouse tables into pandas DataFrames for analysis
- Constructs SQL queries using a syntax that feels like pandas
- Creates views in your DataWarehouse to save transformed data
- Exports runnable sql in .sql files a dbt-compliant yml files
- Offers dozens of free SQL transforms to use
- Coming Soon: allows users to create & add custom transforms

Rasgoql’s initial release will focus on snowflake databases but we plan to add support for BigQuery and Postgres in the very near future. If you'd like to suggest another database type, submit your idea to our [GitHub Discussions page](https://github.com/rasgointelligence/RasgoQL/discussions) so that other community members can weight in and show their support. 

# Can RasgoQL help you?

* If you use pandas to build features, but you are working on a massive set of data that won't fit in your machine's memory. RasgoQL can help!

* If your organization uses dbt of another SQL tool to run production data flows, but you prefer to build features in pandas. RasgoQL can help!

* If you know pandas, but not SQL and want to learn how queries will translate. RasgoQL can help!

# Where to get it
Just run a simple pip install.

`pip install rasgoql`


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

# Advanced Examples

## Joins
Easily join tables together using the `join` transform. 

```python
internet_sales = rasgoql.dataset('INTERNET_SALES')

ds = internet_sales.join(
  join_table='DIM_PRODUCT',
  join_columns={'PRODUCTKEY':'PRODUCTKEY'},
  join_type='LEFT',
  join_prefix='product')
```

## Chain transforms together
Below creates a rolling average aggregation and then drops unnecessary colomns. 

```python
ds_agg = ds.rolling_agg(
    aggregations={"SALESAMOUNT": ["MAX", "MIN", "SUM"]},
    order_by="ORDERDATE",
    offsets=[-7, 7],
    group_by=["PRODUCTKEY"],
).drop_columns(exclude_cols=["ORDERDATEKEY"])
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
```

# Where do I go for help?
If you have any questions please: 

1. [Docs](https://docs.rasgoql.com/)
2. [Slack](https://join.slack.com/t/rasgousergroup/shared_invite/zt-nytkq6np-ANEJvbUSbT2Gkvc8JICp3g)
3. [GitHub Issues](https://github.com/rasgointelligence/RasgoQL/issues)


# How can I contribute? 
Review the [contributors guide](https://github.com/rasgointelligence/RasgoQL/blob/main/CONTRIBUTING.md)


<i>Built for Data Scientists, by Data Scientists</i>

This project is sponspored by RasgoML. Find out at [https://www.rasgoml.com/](https://www.rasgoml.com/)