"""This module exports comfiguration for the current system."""

import os

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env


def boolean_env(environ_name: str, default: str = 'FALSE') -> bool:
    """Helper function."""
    return bool(os.getenv(environ_name, default).upper() in ["1", "Y", "YES", "TRUE"])


IS_OFFLINE = boolean_env(
    'SLS_OFFLINE'
)  # set by serverless-wsgi plugin, and used only when THS is included in a WSGI test
REGION = os.getenv('NZSHM22_HAZARD_STORE_REGION', "us-east-1")
DEPLOYMENT_STAGE = os.getenv('NZSHM22_HAZARD_STORE_STAGE', 'LOCAL').upper()
NUM_BATCH_WORKERS = int(os.getenv('NZSHM22_HAZARD_STORE_NUM_WORKERS', 1))
LOCAL_CACHE_FOLDER = os.getenv('NZSHM22_HAZARD_STORE_LOCAL_CACHE')

SQLITE_ADAPTER_FOLDER = os.getenv('THS_SQLITE_FOLDER')
USE_SQLITE_ADAPTER = boolean_env('THS_USE_SQLITE_ADAPTER')


## SPECIAL SETTINGS FOR MIGRATOIN
SOURCE_REGION = os.getenv('NZSHM22_HAZARD_STORE_MIGRATE_SOURCE_REGION')
SOURCE_DEPLOYMENT_STAGE = os.getenv('NZSHM22_HAZARD_STORE_SOURCE_STAGE')
# TARGET_REGION = os.getenv('NZSHM22_HAZARD_STORE_MIGRATE_TARGET_REGION')
