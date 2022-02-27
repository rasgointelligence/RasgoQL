# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Add
- Add support for Postgres
- Add SQLChain to/from YAML workflow

### Change
- N/A

### Fix
- N/A

### Remove
- N/A

## [1.0.0] - 2022-02-23
### Added
- Added support for BigQuery
- Added support for Snowflake
- Added import dataset from pandas workflow
- Added export to dbt workflow

## [1.0.1] - 2022-02-28
### Added
- Added support for schema.yml dbt file
### Changed
- Changed export to dbt workflow & `SQLChain.to_dbt()` params ([see docs](https://docs.rasgoql.com/workflows/exporting-to-dbt))
- Simplify BigQuery user connection

[1.0.0]: https://pypi.org/project/rasgoql/1.0.0/
[1.0.1]: https://pypi.org/project/rasgoql/1.0.1/
