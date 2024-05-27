"""The HazardRealizationCurve model

with support for model serialisation as pandas/pyarrow datasets
"""

import datetime as dt
import logging

import pytz
from nzshm_common.location.coded_location import CodedLocation
from pynamodb.attributes import ListAttribute, NumberAttribute, UnicodeAttribute
from pynamodb.models import Model
from pynamodb_attributes import FloatAttribute, TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

from ..attributes import EnumConstrainedIntegerAttribute, EnumConstrainedUnicodeAttribute, ForeignKeyAttribute
from ..constraints import IntensityMeasureTypeEnum, VS30Enum
from ..location_indexed_model import datetime_now

log = logging.getLogger(__name__)

VS30_KEYLEN = 4


class HazardRealizationCurve(Model):
    """Stores hazard curve realizations."""

    __metaclass__ = type

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
    producer_config_fk = ForeignKeyAttribute()  # attr_name="prod_conf_fk")
    sources_digest = UnicodeAttribute()
    gmms_digest = UnicodeAttribute()
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)
    vs30 = EnumConstrainedIntegerAttribute(VS30Enum)
    created = TimestampAttribute(default=datetime_now)
    nloc_0 = UnicodeAttribute()  # 0.001deg ~100m grid
    nloc_001 = UnicodeAttribute()  # 0.001deg ~100m grid
    lat = FloatAttribute()  # latitude decimal degrees
    lon = FloatAttribute()  # longitude decimal degrees
    # a reference to where/how this calc done (URI URL, http://nshm-blah-blah/api-ref
    calculation_id = UnicodeAttribute(null=True)

    values = ListAttribute(
        of=NumberAttribute
    )  # corresponding IMT levels are stored in the related HazardCurveProducerConfig

    created = TimestampAttribute(default=datetime_now)

    # def set_location(self, location: CodedLocation):
    #     """Set internal fields, indices etc from the location."""
    #     self.nloc_0 = location.downsample(1.0).code
    #     self.nloc_001 = location.downsample(0.001).code
    #     self.lat = location.lat
    #     self.lon = location.lon
    #     return self

    def build_sort_key(self):
        vs30s = str(self.vs30).zfill(VS30_KEYLEN)
        sort_key = f'{self.nloc_001}:{vs30s}:{self.imt}:'
        sort_key += f'{ForeignKeyAttribute().serialize(self.compatible_calc_fk)}:'
        sort_key += self.sources_digest + ':'
        sort_key += self.gmms_digest
        return sort_key

    def set_location(self, location: CodedLocation):
        """Set internal fields, indices etc from the location."""
        # LocationIndexedModel.set_location(self, location)
        self.nloc_0 = location.downsample(1.0).code
        self.nloc_001 = location.downsample(0.001).code
        self.lat = location.lat
        self.lon = location.lon
        # update the indices
        self.partition_key = self.nloc_0
        self.sort_key = self.build_sort_key()
        return self

    def as_pandas_model(self) -> dict:
        """
        Get the model ready for pandas serialisation
        """
        model = self.to_simple_dict()
        for fld in ['sort_key', 'partition_key']:
            del model[fld]
        model['created'] = dt.datetime.fromtimestamp(model['created'], pytz.timezone("UTC"))
        return model


def get_tables():
    """table classes may be rebased, this makes sure we always get the latest class definition."""
    for cls in [
        globals()['HazardRealizationCurve'],
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
