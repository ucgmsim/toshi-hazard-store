"""This module exports comfiguration for the current system."""

import os


def boolean_env(environ_name: str, default: str = 'FALSE') -> bool:
    """Helper function."""
    return bool(os.getenv(environ_name, default).upper() in ["1", "Y", "YES", "TRUE"])


IS_OFFLINE = boolean_env('SLS_OFFLINE')  # set by serverless-wsgi plugin
REGION = os.getenv('NZSHM22_HAZARD_STORE_REGION', 'us-east-1')
DEPLOYMENT_STAGE = os.getenv('NZSHM22_HAZARD_STORE_STAGE', 'LOCAL').upper()
NUM_BATCH_WORKERS = int(os.getenv('NZSHM22_HAZARD_STORE_NUM_WORKERS', 1))
