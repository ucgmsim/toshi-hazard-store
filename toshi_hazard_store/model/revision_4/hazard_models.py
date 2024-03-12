"""This module defines the pynamodb tables used to store  hazard data. revision 4 = Fourth iteration"""

import logging
import uuid

from nzshm_common.location.code_location import CodedLocation
from pynamodb.attributes import ListAttribute, UnicodeAttribute
from pynamodb.models import Model
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

from ..attributes import ForeignKeyAttribute, IMTValuesAttribute
from ..location_indexed_model import VS30_KEYLEN, LocationIndexedModel, datetime_now

# from toshi_hazard_store.model.caching import ModelCacheMixin


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
    )  # maybe this can be user-defined. a UUID might be too unfriendly for our needs
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

    compatible_calc_fk = ForeignKeyAttribute(
        null=False,  # attr_name='compat_calc_fk'
    )  # must map to a valid CompatibleHazardCalculation.unique_id (maybe wrap in transaction)

    producer_software = UnicodeAttribute()
    producer_version_id = UnicodeAttribute()
    configuration_hash = UnicodeAttribute()
    configuration_data = UnicodeAttribute(null=True)

    notes = UnicodeAttribute(null=True)


class HazardRealizationCurve(LocationIndexedModel):
    """Stores hazard curve realizations."""

    # __metaclass__ = type

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_R4_HazardRealizationCurve-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    partition_key = UnicodeAttribute(hash_key=True)  # a lot of these, let's look at our indexing
    sort_key = UnicodeAttribute(range_key=True)  # e.g ProducerID:MetaID

    compatible_calc_fk = ForeignKeyAttribute(null=False)  # attr_name='compat_calc_fk')
    producer_config_fk = ForeignKeyAttribute(null=False)  # attr_name="prod_conf_fk")

    created = TimestampAttribute(default=datetime_now)
    # vs30 = NumberAttribute()  # vs30 value
    rlz = UnicodeAttribute()  # identifier for the realization in the calculation
    values = ListAttribute(of=IMTValuesAttribute)

    # a reference to where/how this calc done (URI URL, http://nshm-blah-blah/api-ref
    calculation_id = UnicodeAttribute(null=True)

    branch_sources = UnicodeAttribute(
        null=True
    )  # we need this as a sorted string for searching (NSHM will use nrml/source_id for now)
    branch_gmms = UnicodeAttribute(null=True)  #

    # Secondary Index attributes
    # index1 = vs30_nloc1_gt_rlz_index()
    # index1_rk = UnicodeAttribute()

    def set_location(self, location: CodedLocation):
        """Set internal fields, indices etc from the location."""
        # print(type(self).__bases__)
        LocationIndexedModel.set_location(self, location)
        # super(LocationIndexedModel, self).set_location(location)

        # update the indices
        rlzs = str(self.rlz).zfill(6)

        vs30s = str(self.vs30).zfill(VS30_KEYLEN)
        self.partition_key = self.nloc_1
        self.sort_key = f'{self.nloc_001}:{vs30s}:{rlzs}:'
        self.sort_key += f'{ForeignKeyAttribute().serialize(self.compatible_calc_fk)}:'
        self.sort_key += f'{ForeignKeyAttribute().serialize(self.producer_config_fk)}'
        # self.index1_rk = f'{self.nloc_1}:{vs30s}:{rlzs}:{self.hazard_solution_id}'
        return self


def get_tables():
    """table classes may be rebased, this makes sure we always get the latest class definition."""
    for cls in [
        globals()['CompatibleHazardCalculation'],
        globals()['HazardCurveProducerConfig'],
        # globals()['HazardRealizationMeta'],
        globals()['HazardRealizationCurve'],
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
