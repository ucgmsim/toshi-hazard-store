# Changelog

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
