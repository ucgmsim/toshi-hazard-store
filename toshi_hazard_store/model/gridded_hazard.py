"""This module defines the pynamodb tables used to store THH."""

import logging
from datetime import datetime, timezone

from pynamodb.attributes import UnicodeAttribute, VersionAttribute
from pynamodb.models import Model
from pynamodb_attributes import FloatAttribute, TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION  # we can share THS settings for model

from .attributes import CompressedListAttribute, EnumConstrainedIntegerAttribute, EnumConstrainedUnicodeAttribute
from .constraints import AggregationEnum, IntensityMeasureTypeEnum, VS30Enum


def datetime_now():
    return datetime.now(tz=timezone.utc)


log = logging.getLogger(__name__)


class GriddedHazard(Model):
    """Grid points defined in location_grid_id has a values in grid_poes."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_GriddedHazard-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    partition_key = UnicodeAttribute(hash_key=True)
    sort_key = UnicodeAttribute(range_key=True)

    version = VersionAttribute()
    created = TimestampAttribute(default=datetime_now)

    hazard_model_id = UnicodeAttribute()
    location_grid_id = UnicodeAttribute()

    vs30 = EnumConstrainedIntegerAttribute(VS30Enum)
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)
    agg = EnumConstrainedUnicodeAttribute(AggregationEnum)
    poe = FloatAttribute()

    grid_poes = CompressedListAttribute()

    @staticmethod
    def new_model(hazard_model_id, location_grid_id, vs30, imt, agg, poe, grid_poes) -> 'GriddedHazard':
        obj = GriddedHazard(
            hazard_model_id=hazard_model_id,
            location_grid_id=location_grid_id,
            vs30=vs30,
            imt=imt,
            agg=agg,
            poe=poe,
            grid_poes=grid_poes,
        )
        obj.partition_key = f"{obj.hazard_model_id}"
        obj.sort_key = f"{obj.hazard_model_id}:{obj.location_grid_id}:{obj.vs30}:{obj.imt}:{obj.agg}:{obj.poe}"
        return obj


tables = [GriddedHazard]


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
