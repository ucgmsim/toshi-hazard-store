# Changelog

## [0.8.0] - 2024-02
### Added
 - db_adapter architecture
 - sqlite3 as db_adapter for localstorage option
 - new envionment varisbale for localstorage
 - more documentation
 - use tmp_path for new localstorage tests
 - db_adapter supports SS field type
 - dynamodb unique behaviour implement in sqlite
 - support for .env configuration (using python-dotenv)

### Changed
  - update openquake dependency for NSHM GSIMs
  - drop python 3.8 and update deps for openquake
  - more test coverage
  - refactor tests to use temporary folders correctly

## [0.7.8] - 2024-01-31
### Added
 - 0.5% in 50 years PoE for disaggregations

## [0.7.7] - 2023-12-13
### Changed 
 - fix publication workflow
 
## [0.7.6] - 2023-12-07
### Changed
 - update pandas dependency to ~2.0.3

## [0.7.5] - 2023-08-21
### Changed
 - faster queries for THS_OpenquakeMeta table

## [0.7.4] - 2023-08-17
### Changed
 - faster queries for THS_OpenquakeRealization table

## [0.7.3] - 2023-08-15
### Removed
 - support for python 3.8

### Changed
 - faster queries for THS_HazardAggregation table
 - query optimisation to gridded_hazard_query
 - query optimisation to disagg_querys
 - mypy 1.5.0
 - pynamodb 5.5.0
 - update mkdocs toolchain
 - GHA scripts install with `--extra openquake`

### Added 
 - ths_testing script for evaluation of performance changes
 - python 3.11
  
## [0.7.2] - 2023-04-24
### Changed
- use poetry 1.4.2 for release build workflow

## [0.7.1] - 2023-04-24
### Changed
- update nzshm-common dependency 0.6.0
- mock cache when testing hazard aggregation query

### Removed
- remove version control for ToshiOpenquakeMeta

## [0.7.0] - 2023-04-17
### Changed
 - update openquake dependency 3.16
 - update nzshm-common dependency 0.5.0
 - fix SA(0.7) value

### Added
 - script ths_cache to prepopulate and test caching
 - local caching feature
 - more spectral periods in constraint enum
 - new constraints to existing THS models
 - fix enum validations and apply to model fields
### Removed
  - remove v2 type options from batch save

## [0.6.0] - 2023-02-15
### Changed
 - refactor model package
 - refactor model.attributes package
 - more test coverage
### Added
 - two new models for DisaggAggregations
 - validation via Enum for aggregation values
 - new enumerations and constraints for probabilities, IMTS and VS30s

## [0.5.5] - 2022-10-06
### Changed
 - fix for queries with mixed length vs30 index keys
 - migrate more print statements to logging.debug

## [0.5.4] - 2022-09-27
### Added
 - new query get_one_gridded_hazard
 - -m option to script to export meta tables only
### Changed
 - migrated print statements to logging.debug
 - removed monkey patch for BASE183 - it iss in oqengine 3.15 now
 - more test cover

## [0.5.3] - 2022-08-18
### Changed
 - using nzshm-common==0.3.2 from pypi.
 - specify poetry==1.2.0b3 in all the GHA yml files.

## [0.5.1] - 2022-08-17
### Added
 - THS_HazardAggregation table support for csv serialisation.
### Changed
 - refactoring/renaming openquake import modules.
### Removed
 - one openquake test no longer works as expected. It's off-piste so skipping it for now.
 - data_functions migrated to THP
 - branch_combinator migrated to THP

## [0.5.0] - 2022-08-03
### Added
 - V3 THS table models with improved indexing and and performance (esp. THS_HazardAggregation table)
 - using latest CodedLocation API to manage gridded lcoations and resampling.
### Removed
 -  realisation aggregration computations. These have moving to toshi-hazard-post

## [0.4.1] - 2022-06-22
### Added
 - multi_batch module for parallelised batch saves
 - DESIGN.md capture notes on the experiments, test and mods to the package
 - new switch on V2 queries to force normalised_location_id
 - new '-f' switch on store_hazard script to force normalised_location_id
 - lat, lon Float fields to support numeric range filtering in queries
 - created timestamp field on stas, rlzs v2
 - added pynamodb_attributes for FloatAttribute, TimestampAttribute types

### Changed
 - V2 store queries will automatically use nomralised location if custom sites aren't available.
 - refactored model modules.

## [0.4.0] - 2022-06-10
### Added
 - new V2 models for stats and rlzs.
 - new get_hazard script for manual testing.
 - extra test coverage with optional openquake install as DEV dependency.

### Changed
 - meta dataframes are cut back to dstore defaults to minimise size.

## [0.3.2] - 2022-05-30
### Added
 - meta.aggs attribute
 - meta.inv_tme attribute

### Changed
 - store hazard can create tables.
 - store hazard adds extra meta.
 - store hazard truncates values for rlz and agg fields.
 - make stats & rlz queries tolerant to ID-only form (fails with REAL dynamodb & not in mocks).

## [0.3.1] - 2022-05-29
### Changed
 - updated usage.

## [0.3.0] - 2022-05-28
### Added
 - store_hazard script for openquake systems.
### Changed
 - tightened up model attributes names.

## [0.2.0] - 2022-05-27
### Added
 - query api improvements
 - added meta table
 - new query methods for meta and rlzs

### Changed
 - moved vs30 from curves to meta
 - updated docs

## [0.1.3] - 2022-05-26
### Changed
 - fixed mkdoc rendering of python & markdown.


## [0.1.2] - 2022-05-26
### Changed
 - fix poetry lockfile

## [0.1.1] - 2022-05-26
### Added
 - First release on PyPI.
 - query and model modules providing basic support for openquake hazard stats curves only.
