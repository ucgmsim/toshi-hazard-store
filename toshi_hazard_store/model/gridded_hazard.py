"""This module defines the pynamodb tables used to store THH."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

from nzshm_common.util import compress_string, decompress_string
from pynamodb.attributes import Attribute, UnicodeAttribute, VersionAttribute
from pynamodb.constants import STRING
from pynamodb.models import Model
from pynamodb_attributes import FloatAttribute, TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION  # we can share THS settings for model


def datetime_now():
    return datetime.now(tz=timezone.utc)


log = logging.getLogger(__name__)


class CompressedJsonicAttribute(Attribute):
    """
    A compressed, json serialisable model attribute
    """

    attr_type = STRING

    def serialize(self, value: Any) -> str:
        return compress_string(json.dumps(value))  # could this be pickle??

    def deserialize(self, value: str) -> Union[Dict, List]:
        return json.loads(decompress_string(value))


class CompressedListAttribute(CompressedJsonicAttribute):
    """
    A compressed list of floats attribute.
    """

    def serialize(self, value: List[float]) -> str:
        if value is not None and not isinstance(value, list):
            raise TypeError(
                f"value has invalid type '{type(value)}'; List[float])expected",
            )
        return super().serialize(value)


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

    vs30 = FloatAttribute()
    imt = UnicodeAttribute()
    agg = UnicodeAttribute()
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
