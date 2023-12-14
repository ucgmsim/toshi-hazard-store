"""
defines the pynamodb tables used to store openquake data.

Version 2
"""

import logging

from pynamodb.attributes import NumberAttribute, UnicodeAttribute, UnicodeSetAttribute  # noqa

# from pynamodb.indexes import AllProjection, LocalSecondaryIndex
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION
from toshi_hazard_store.v2.db_adapter import ModelAdapterMixin, sqlite_adapter

from ...model.location_indexed_model import datetime_now

# from typing import Iterable, Iterator, Sequence, Union


log = logging.getLogger(__name__)


class ToshiV2DemoTable(ModelAdapterMixin):
    """Stores metadata from the job configuration and the oq HDF5."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"ToshiV2_DemoTable-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    class AdapterMeta:
        adapter = sqlite_adapter.SqliteAdapter  # the database adapter implementation

    hash_key = UnicodeAttribute(hash_key=True)
    range_rk = UnicodeAttribute(range_key=True)

    created = TimestampAttribute(default=datetime_now)

    hazard_solution_id = UnicodeAttribute()
    general_task_id = UnicodeAttribute()
    vs30 = NumberAttribute()

    imts = UnicodeSetAttribute()  # list of IMTs


tables = [
    ToshiV2DemoTable,
]


def migrate():
    """Create the tables, unless they exist already."""
    for table in tables:
        if not table.exists():  # pragma: no cover
            table.create_table(wait=True)
            log.info(f"Migrate created table: {table}")


def drop_tables():
    """Drop the tables, if they exist."""
    for table in tables:
        if table.exists():  # pragma: no cover
            table.delete_table()
            log.info(f'deleted table: {table}')
