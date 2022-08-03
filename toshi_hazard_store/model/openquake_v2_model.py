"""This module defines the pynamodb tables used to store openquake v2 data."""

import logging
from datetime import datetime, timezone

from pynamodb.attributes import ListAttribute, MapAttribute, NumberAttribute, UnicodeAttribute, VersionAttribute
from pynamodb.models import Model
from pynamodb_attributes import FloatAttribute, TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

log = logging.getLogger(__name__)


def datetime_now():
    return datetime.now(tz=timezone.utc)


class IMTValuesAttribute(MapAttribute):
    """Store the IntensityMeasureType e.g.(PGA, SA(N)) and the levels and values lists."""

    imt = UnicodeAttribute()
    lvls = ListAttribute(of=NumberAttribute)
    vals = ListAttribute(of=NumberAttribute)


class ToshiOpenquakeHazardCurveRlzsV2(Model):
    """Stores the individual hazard realisation curves."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"ToshiOpenquakeHazardCurveRlzsV2-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    haz_sol_id = UnicodeAttribute(hash_key=True)
    loc_rlz_rk = UnicodeAttribute(range_key=True)  # TODO: check we can actually use this in queries!

    loc = UnicodeAttribute()
    rlz = UnicodeAttribute()
    lat = FloatAttribute()
    lon = FloatAttribute()
    created = TimestampAttribute(default=datetime_now)

    values = ListAttribute(of=IMTValuesAttribute)
    version = VersionAttribute()


class ToshiOpenquakeHazardCurveStatsV2(Model):
    """Stores the individual hazard statistical curves."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"ToshiOpenquakeHazardCurveStatsV2-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    haz_sol_id = UnicodeAttribute(hash_key=True)
    loc_agg_rk = UnicodeAttribute(range_key=True)

    loc = UnicodeAttribute()
    agg = UnicodeAttribute()
    lat = FloatAttribute()
    lon = FloatAttribute()
    created = TimestampAttribute(default=datetime_now)

    values = ListAttribute(of=IMTValuesAttribute)
    version = VersionAttribute()


tables = [
    ToshiOpenquakeHazardCurveRlzsV2,
    ToshiOpenquakeHazardCurveStatsV2,
]


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
