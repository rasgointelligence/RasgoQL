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


[1.0.0]: https://pypi.org/project/rasgoql/1.0.0/
[1.0.1]: https://pypi.org/project/rasgoql/1.0.1/
[1.0.2]: https://pypi.org/project/rasgoql/1.0.2/
[1.1.0]: https://pypi.org/project/rasgoql/1.1.0/
