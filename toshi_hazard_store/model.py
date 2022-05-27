"""This module defines the pynamodb tables used to store openquake data."""

import logging

from pynamodb.attributes import (
    JSONAttribute,
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    UnicodeAttribute,
    UnicodeSetAttribute,
    UTCDateTimeAttribute,
    VersionAttribute,
)
from pynamodb.models import Model

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

log = logging.getLogger(__name__)


class ToshiOpenquakeHazardMeta(Model):
    """Stores metadata from the job configuration and the oq HDF5."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"ToshiOpenquakeHazardMeta-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    partition_key = UnicodeAttribute(hash_key=True)  # a static value as we actually don't want to partition our data
    hazsol_vs30_rk = UnicodeAttribute(range_key=True)

    updated = UTCDateTimeAttribute()
    version = VersionAttribute()

    # known at configuration
    hazard_solution_id = UnicodeAttribute()
    vs30 = NumberAttribute()  # vs30 value
    imt_codes = UnicodeSetAttribute()  # list of IMTs
    loc_codes = UnicodeSetAttribute()  # list of Location codes
    source_models = UnicodeSetAttribute()  # list of source model ids

    # extracted from the OQ HDF5
    source_df = JSONAttribute()  # sources meta as DataFrame JSON
    gsim_df = JSONAttribute()  # gmpe meta as DataFrame JSON
    rlzs_df = JSONAttribute()  # realization meta as DataFrame JSON


class LevelValuePairAttribute(MapAttribute):
    """Store the IMT level and the POE value at the level."""

    level = NumberAttribute(null=False)
    value = NumberAttribute(null=False)


class ToshiOpenquakeHazardCurveRlzs(Model):
    """Stores the individual hazard realisation curves."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"ToshiOpenquakeHazardCurveRlzs-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    hazard_solution_id = UnicodeAttribute(hash_key=True)
    imt_loc_rlz_rk = UnicodeAttribute(range_key=True)  # TODO: check we can actually use this in queries!

    imt_code = UnicodeAttribute()
    location_code = UnicodeAttribute()
    rlz_id = UnicodeAttribute()

    lvl_val_pairs = ListAttribute(of=LevelValuePairAttribute)
    version = VersionAttribute()


class ToshiOpenquakeHazardCurveStats(Model):
    """Stores the individual hazard statistical curves."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"ToshiOpenquakeHazardCurveStats-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    hazard_solution_id = UnicodeAttribute(hash_key=True)
    imt_loc_agg_rk = UnicodeAttribute(range_key=True)

    imt_code = UnicodeAttribute()
    location_code = UnicodeAttribute()
    aggregation = UnicodeAttribute()

    lvl_val_pairs = ListAttribute(of=LevelValuePairAttribute)
    version = VersionAttribute()


tables = [ToshiOpenquakeHazardCurveRlzs, ToshiOpenquakeHazardCurveStats, ToshiOpenquakeHazardMeta]


def migrate():
    """Create the tables, unless they exist already."""
    for table in tables:
        if not table.exists():  # pragma: no cover
            table.create_table(wait=True)
            print(f"Migrate created table: {table}")
            log.info(f"Migrate created table: {table}")


def drop_tables():
    """Drop the tables, if they exist."""
    for table in tables:
        if table.exists():  # pragma: no cover
            table.delete_table()
            log.info(f'deleted table: {table}')
