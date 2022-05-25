"""This module defines the pynamodb tables used to store openquake data."""

from nzshm_oq_export.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION
from pynamodb.attributes import JSONAttribute, UnicodeAttribute, VersionAttribute  # NumberAttribute
from pynamodb.models import Model


class ToshiHazardCurveObject(Model):
    """This table store a single hazard curve."""

    class Meta:
        """Meta."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"OpenquakeToshiHazardCurveObject-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"

    object_id = UnicodeAttribute(hash_key=True)  # this will be a composite key
    hazard_solution_id = UnicodeAttribute()
    object_content = JSONAttribute()  # the json string
    version = VersionAttribute()


tables = [ToshiHazardCurveObject]


def migrate():
    """Create the tables if it doesn't exist already."""
    for table in tables:
        if not table.exists():
            table.create_table(wait=True)
            print(f"Migrate created table: {table}")


def drop_tables():
    """Drop the tables if they exist."""
    for table in tables:
        if table.exists():
            table.delete_table()
            print(f'deleted table: {table}')
