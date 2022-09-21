# Changelog

## [0.5.4]

### Added
 - new query get_one_gridded_hazard

### Changed
 - improving logging
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
