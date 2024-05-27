"""This module defines the pynamodb tables used to store  hazard data. revision 4 = Fourth iteration"""

import logging

from pynamodb.attributes import ListAttribute, NumberAttribute, UnicodeAttribute, VersionAttribute
from pynamodb.models import Model
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

from ..attributes import ForeignKeyAttribute
from ..location_indexed_model import datetime_now
from .hazard_realization_curve import HazardRealizationCurve  # noqa: F401

log = logging.getLogger(__name__)

VS30_KEYLEN = 4

# HazardRealizationCurve = hazard_realization_curve.HazardRealizationCurve


class CompatibleHazardCalculation(Model):
    """Provides a unique identifier for compatabile Hazard Calculations"""

    __metaclass__ = type

    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_R4_CompatibleHazardCalculation-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    partition_key = UnicodeAttribute(hash_key=True)  # a static value as we actually don't want to partition our data
    uniq_id = UnicodeAttribute(range_key=True)  # user-defined. since a UUID might be too unfriendly for our needs
    notes = UnicodeAttribute(null=True)
    created = TimestampAttribute(default=datetime_now)

    def foreign_key(self):
        return (str(self.partition_key), str(self.uniq_id))


class HazardCurveProducerConfig(Model):
    """Records characteristics of Hazard Curve producers/engines for compatablitiy tracking"""

    __metaclass__ = type

    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_R4_HazardCurveProducerConfig-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    partition_key = UnicodeAttribute(hash_key=True)  # a static value as we actually don't want to partition our data
    range_key = UnicodeAttribute(range_key=True)  # combination of the unique configuration identifiers
    version = VersionAttribute()

    compatible_calc_fk = ForeignKeyAttribute(
        null=False,  # attr_name='compat_calc_fk'
    )  # must map to a valid CompatibleHazardCalculation.unique_id (maybe wrap in transaction)

    created = TimestampAttribute(default=datetime_now)
    modified = TimestampAttribute(default=datetime_now)

    effective_from = TimestampAttribute(null=True)
    last_used = TimestampAttribute(null=True)

    tags = ListAttribute(of=UnicodeAttribute, null=True)

    producer_software = UnicodeAttribute()
    producer_version_id = UnicodeAttribute()
    configuration_hash = UnicodeAttribute()
    configuration_data = UnicodeAttribute(null=True)

    imts = ListAttribute(of=UnicodeAttribute)  # EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum))
    imt_levels = ListAttribute(of=NumberAttribute)
    notes = UnicodeAttribute(null=True)

    def foreign_key(self):
        return (str(self.partition_key), str(self.range_key))


def get_tables():
    """table classes may be rebased, this makes sure we always get the latest class definition."""
    # print(globals())
    for cls in [
        globals()['CompatibleHazardCalculation'],
        globals()['HazardCurveProducerConfig'],
        # # globals()['HazardRealizationMeta'],
        # HazardRealizationCurve,
    ]:
        yield cls


def migrate():
    """Create the tables, unless they exist already."""
    for table in get_tables():
        print(table.__bases__)
        if not table.exists():  # pragma: no cover
            table.create_table(wait=True)
            log.info(f"Migrate created table: {table}")


def drop_tables():
    """Drop the tables, if they exist."""
    for table in get_tables():
        if table.exists():  # pragma: no cover
            table.delete_table()
            log.info(f'deleted table: {table}')
