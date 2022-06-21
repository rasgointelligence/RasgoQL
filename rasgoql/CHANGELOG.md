# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Add
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
- Changed auth workflow and `BigQueryCredentials` params ([see docs](https://docs.rasgoql.com/datawarehouses/bigquery))

## [1.0.2] - 2022-03-03
## Added
- Added `acknowledge_risk` parameter to `.query()` and `.query_into_df()` functions
- Added support for loading external and temporary tables as Datasets

## Changed
- Loosened restriction on what constitutes a valid fqtn
    - FROM: `\w+\.\w+\.\w+` (mandate: word.word.word)
    - TO: `^[^\s]+\.[^\s]+\.[^\s]+` (mandate: anychars.anychars.anychars)

### Fixed
- Fixed bug that loaded account param as a list when calling `SnowflakeCredentials.from_env()`

## [1.1.0] - 2022-03-08
### Added
- Added support for Postgres

### Changed
- made `name` a mandatory parameter in SQLChain and Dataset `.transform()` methods

## [1.1.1] - 2022-03-16
### Fixed
- Fixed a bug where `change_namespace()` methods of Transforms and DataWarehouses failed

## [1.2.0] - 2022-03-24
### Added
- Added a parameter to allow batch returning of Pandas DataFrames from `to_df()` and `query_into_df()` methods

## [1.3.0] - 2022-03-25
### Added
- Added support for MySQL

## [1.3.1] - 2022-04-04
### Added
- Added `tags` property to `TransformTemplate` class
### Removed
- Removed `transform_type` property from `TransformTemplate` class

## [1.4.0] - 2022-04-08
### Added
- Added failure and execution telemetry for methods in Transform and Dataset classes

## [1.5.0] - 2022-04-11
### Added
- Added support for Amazon Redshift
### Removed
- Removed `check_data_warehouse` function

## [1.5.1] - 2022-04-13
### Fixed
- Support for redshift extras during pip install

## [1.5.2] - 2022-04-29
### Fixed
- Fixing get_object_details for postgres

## [1.5.3] - 2022-05-20
### Added
- Added `get_columns` to list of source functions

## [1.5.4] - 2022-05-23
### Added
- Added db-dtypes as a dependency to optional rasgoql[bigquery] package to prevent breaking changes in google-cloud-bigquery v3.0.0

## [1.5.5] - 2022-06-07
### Fixed
- Fixed a bug where get_columns would not render for all DW types

## [1.5.6] - 2022-06-20
### Changed
- Changed the `get_schema` method on Snowflake and BigQuery DW classes to get output columns without creating views


[1.0.0]: https://pypi.org/project/rasgoql/1.0.0/
[1.0.1]: https://pypi.org/project/rasgoql/1.0.1/
[1.0.2]: https://pypi.org/project/rasgoql/1.0.2/
[1.1.0]: https://pypi.org/project/rasgoql/1.1.0/
[1.1.1]: https://pypi.org/project/rasgoql/1.1.1/
[1.2.0]: https://pypi.org/project/rasgoql/1.2.0/
[1.3.0]: https://pypi.org/project/rasgoql/1.3.0/
[1.3.1]: https://pypi.org/project/rasgoql/1.3.1/
[1.4.0]: https://pypi.org/project/rasgoql/1.4.0/
[1.5.0]: https://pypi.org/project/rasgoql/1.5.0/
[1.5.1]: https://pypi.org/project/rasgoql/1.5.1/
[1.5.2]: https://pypi.org/project/rasgoql/1.5.2/
[1.5.3]: https://pypi.org/project/rasgoql/1.5.3/
[1.5.4]: https://pypi.org/project/rasgoql/1.5.4/
[1.5.5]: https://pypi.org/project/rasgoql/1.5.5/
[1.5.6]: https://pypi.org/project/rasgoql/1.5.6/
