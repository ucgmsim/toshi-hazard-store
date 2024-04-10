"""This module defines the pynamodb tables used to store  hazard data. revision 4 = Fourth iteration"""

import logging

from nzshm_common.location.code_location import CodedLocation
from pynamodb.attributes import ListAttribute, NumberAttribute, UnicodeAttribute, VersionAttribute
from pynamodb.models import Model
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

from ..attributes import EnumConstrainedUnicodeAttribute, ForeignKeyAttribute
from ..constraints import IntensityMeasureTypeEnum
from ..location_indexed_model import LocationIndexedModel, datetime_now

# from toshi_hazard_store.model.caching import ModelCacheMixin


log = logging.getLogger(__name__)

VS30_KEYLEN = 4


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

    compatible_calc_fk = ForeignKeyAttribute()
    sources_digest = UnicodeAttribute()
    gmms_digest = UnicodeAttribute()
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)

    created = TimestampAttribute(default=datetime_now)
    producer_config_fk = ForeignKeyAttribute()  # attr_name="prod_conf_fk")

    values = ListAttribute(
        of=NumberAttribute
    )  # corresponding IMT levels are stored in the related HazardCurveProducerConfig

    # a reference to where/how this calc done (URI URL, http://nshm-blah-blah/api-ref
    calculation_id = UnicodeAttribute(null=True)

    # def _sources_key(self):
    #     return "s" + "|".join(self.source_digests)

    # def _gmms_key(self):
    #     return "g" + "|".join(self.gmm_digests)

    def build_sort_key(self):
        vs30s = str(self.vs30).zfill(VS30_KEYLEN)
        sort_key = f'{self.nloc_001}:{vs30s}:{self.imt}:'
        sort_key += f'{ForeignKeyAttribute().serialize(self.compatible_calc_fk)}:'
        sort_key += self.sources_digest + ':'
        sort_key += self.gmms_digest
        return sort_key

    def set_location(self, location: CodedLocation):
        """Set internal fields, indices etc from the location."""
        LocationIndexedModel.set_location(self, location)
        # update the indices
        self.partition_key = self.nloc_1
        self.sort_key = self.build_sort_key()
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
