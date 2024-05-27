#! python3
# flake8: noqa: F401
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:

    # from toshi_hazard_store.config import *
    from toshi_hazard_store.config import DEPLOYMENT_STAGE
    from toshi_hazard_store.config import DEPLOYMENT_STAGE as THS_STAGE
    from toshi_hazard_store.config import LOCAL_CACHE_FOLDER, NUM_BATCH_WORKERS
    from toshi_hazard_store.config import REGION
    from toshi_hazard_store.config import REGION as THS_REGION
    from toshi_hazard_store.config import USE_SQLITE_ADAPTER

    API_URL = None


def echo_settings(work_folder: str, verbose=True):
    global click
    global DEPLOYMENT_STAGE, API_URL, REGION, LOCAL_CACHE_FOLDER, THS_STAGE, THS_REGION, USE_SQLITE_ADAPTER

    click.echo('\nfrom command line:')
    click.echo(f"   using verbose: {verbose}")
    click.echo(f"   using work_folder: {work_folder}")

    try:
        click.echo('\nfrom API environment:')
        click.echo(f'   using API_URL: {API_URL}')
        click.echo(f'   using REGION: {REGION}')
        click.echo(f'   using DEPLOYMENT_STAGE: {DEPLOYMENT_STAGE}')
    except Exception:
        pass

    click.echo('\nfrom THS config:')
    # click.echo(f'   using LOCAL_CACHE_FOLDER: {LOCAL_CACHE_FOLDER}')
    click.echo(f'   using THS_STAGE: {THS_STAGE}')
    click.echo(f'   using THS_REGION: {THS_REGION}')
    click.echo(f'   using USE_SQLITE_ADAPTER: {USE_SQLITE_ADAPTER}')
