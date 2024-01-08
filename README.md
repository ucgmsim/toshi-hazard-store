# toshi-hazard-store


[![pypi](https://img.shields.io/pypi/v/toshi-hazard-store.svg)](https://pypi.org/project/toshi-hazard-store/)
[![python](https://img.shields.io/pypi/pyversions/toshi-hazard-store.svg)](https://pypi.org/project/toshi-hazard-store/)
[![Build Status](https://github.com/GNS-Science/toshi-hazard-store/actions/workflows/dev.yml/badge.svg)](https://github.com/GNS-Science/toshi-hazard-store/actions/workflows/dev.yml)
[![codecov](https://codecov.io/gh/GNS-Science/toshi-hazard-store/branch/main/graphs/badge.svg)](https://codecov.io/github/GNS-Science/toshi-hazard-store)


* Documentation: <https://GNS-Science.github.io/toshi-hazard-store>
* GitHub: <https://github.com/GNS-Science/toshi-hazard-store>
* PyPI: <https://pypi.org/project/toshi-hazard-store/>
* Free software: GPL-3.0-only

## Features

* Manage Openquake hazard results in AWS DynamodDB tables defined herein.
* Option for caching using sqlite, See NZSHM22_HAZARD_STORE_LOCAL_CACHE environment variable.
* Option to use a local sqlite store instead of DynamoDB, see THS_USE_SQLITE_ADAPTER and THS_SQLITE_FOLDER variables.
* cli tools for end users

## Credits

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [waynerv/cookiecutter-pypackage](https://github.com/waynerv/cookiecutter-pypackage) project template.
