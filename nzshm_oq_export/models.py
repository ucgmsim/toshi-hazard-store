"""This module defines the pynamodb tables used to store openquake data."""

import logging

from pynamodb.attributes import ListAttribute, MapAttribute, NumberAttribute, UnicodeAttribute, VersionAttribute
from pynamodb.models import Model

from nzshm_oq_export.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

log = logging.getLogger(__name__)


class LevelValuePairAttribute(MapAttribute):
    """Store the IMT level and the value at the level."""

    level = NumberAttribute(null=False)
    value = NumberAttribute(null=False)


class ToshiHazardCurveRlzsObject(Model):
    """Stores the individual hazard realisation curves."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"ToshiHazardCurveRlzsObject-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"

    hazard_solution_id = UnicodeAttribute(hash_key=True)  # this
    loc_imt_rk = UnicodeAttribute(range_key=True)

    location_code = UnicodeAttribute()
    imt_code = UnicodeAttribute()
    lvl_val_pairs = ListAttribute(of=LevelValuePairAttribute)
    version = VersionAttribute()


# class ToshiHazardCurveStatsObject(Model):
#     """This table stores the individual hazard statistal curves."""

#     class Meta:
#         """Meta."""

#         billing_mode = 'PAY_PER_REQUEST'
#         table_name = f"ToshiHazardCurveRlzsObject-{DEPLOYMENT_STAGE}"
#         region = REGION
#         if IS_OFFLINE:
#             host = "http://localhost:8000"

#     hazard_solution_id = UnicodeAttribute(hash_key=True)      #this
#     loc_imt_rk = UnicodeAttribute(range_key=True)

#     location_code = UnicodeAttribute()
#     imt_code =  UnicodeAttribute()
#     lvl_val_pairs = ListAttribute(of=LevelValuePairAttribute)
#     version = VersionAttribute()


tables = [ToshiHazardCurveRlzsObject]


def migrate():
    """Create the tables if it doesn't exist already."""
    for table in tables:
        if not table.exists():
            table.create_table(wait=True)
            print(f"Migrate created table: {table}")
            log.info(f"Migrate created table: {table}")


def drop_tables():
    """Drop the tables if they exist."""
    for table in tables:
        if table.exists():
            table.delete_table()
            print(f'deleted table: {table}')


# def set_local_mode(host="http://localhost:8000"):
#     """Used for offline testing (depends on a local SLS service)."""
#     log.info(f"Setting tables for local dynamodb instance in offline mode")
#     for table in table_classes:
#         table.Meta.host = host
