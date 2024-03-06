"""This module defines the pynamodb tables used to store  hazard data. revision 4 = Fourth iteration"""

import logging
import uuid
from typing import Iterable, Iterator, Sequence, Union

from nzshm_common.location.code_location import CodedLocation
from pynamodb.attributes import JSONAttribute, ListAttribute, NumberAttribute, UnicodeAttribute, UnicodeSetAttribute
from pynamodb.models import Model
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION
from toshi_hazard_store.model.caching import ModelCacheMixin
from ..location_indexed_model import datetime_now  # VS30_KEYLEN, LocationIndexedModel,


log = logging.getLogger(__name__)


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
    uniq_id = UnicodeAttribute(
        range_key=True, default=str(uuid.uuid4())
    )  #  maybe this can be user-defined. a UUID might be too unfriendly for our needs
    notes = UnicodeAttribute(null=True)


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
    compat_calc_fk = UnicodeAttribute(
        null=False
    )  # must map to a valid CompatibleHazardCalculation.uniq_id (maybe wrap in transaction)

    producer_software = UnicodeAttribute()
    producer_version_id = UnicodeAttribute()
    configuration_hash = UnicodeAttribute()
    configuration_data = UnicodeAttribute(null=True)

    notes = UnicodeAttribute(null=True)


class HazardRealizationMeta(Model):
    """Stores metadata from a hazard calculation run - nothing OQ specific here please."""

    __metaclass__ = type

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_R4_HazardRealizationMeta-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    partition_key = UnicodeAttribute(hash_key=True)  # a static value as we actually don't want to partition our data
    range_key = UnicodeAttribute(range_key=True)
    compat_calc_fk = UnicodeAttribute(
        null=False
    )  # must map to a valid CompatibleHazardCalculation.unique_id (maybe wrap in transaction)
    config_fk = UnicodeAttribute(
        null=False
    )  # must map to a valid HazardCurveProducerConfig.unique_id (maybe wrap in transaction)

    created = TimestampAttribute(default=datetime_now)
    vs30 = NumberAttribute()  # vs30 value


#     hazsol_vs30_rk = UnicodeAttribute(range_key=True)

#     created = TimestampAttribute(default=datetime_now)

#     hazard_solution_id = UnicodeAttribute()
#     general_task_id = UnicodeAttribute()
#     vs30 = NumberAttribute()  # vs30 value

#     imts = UnicodeSetAttribute()  # list of IMTs
#     locations_id = UnicodeAttribute()  # Location codes identifier (ENUM?)
#     source_ids = UnicodeSetAttribute()
#     source_tags = UnicodeSetAttribute()
#     inv_time = NumberAttribute()  # Invesigation time in years

#     # extracted from the OQ HDF5
#     src_lt = JSONAttribute()  # sources meta as DataFrame JSON
#     gsim_lt = JSONAttribute()  # gmpe meta as DataFrame JSON
#     rlz_lt = JSONAttribute()  # realization meta as DataFrame JSON


def get_tables():
    """table classes may be rebased, this makes sure we always get the latest class definition."""
    for cls in [
        globals()['CompatibleHazardCalculation'],
        globals()['HazardCurveProducerConfig'],
        globals()['HazardRealizationMeta'],
    ]:
        yield cls


def migrate():
    """Create the tables, unless they exist already."""
    for table in get_tables():
        if not table.exists():  # pragma: no cover
            table.create_table(wait=True)
            log.info(f"Migrate created table: {table}")


def drop_tables():
    """Drop the tables, if they exist."""
    for table in get_tables():
        if table.exists():  # pragma: no cover
            table.delete_table()
            log.info(f'deleted table: {table}')
