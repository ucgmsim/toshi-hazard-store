"""This module defines the pynamodb tables used to store openquake data. Third iteration"""

import logging
from typing import Iterable, Iterator, Sequence, Union

from nzshm_common.location.code_location import CodedLocation
from pynamodb.attributes import JSONAttribute, ListAttribute, NumberAttribute, UnicodeAttribute, UnicodeSetAttribute
from pynamodb.indexes import AllProjection, LocalSecondaryIndex
from pynamodb.models import Model
from pynamodb_attributes import IntegerAttribute, TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION
from toshi_hazard_store.model.caching import ModelCacheMixin

from .attributes import EnumConstrainedUnicodeAttribute, IMTValuesAttribute, LevelValuePairAttribute
from .constraints import AggregationEnum, IntensityMeasureTypeEnum
from .location_indexed_model import VS30_KEYLEN, LocationIndexedModel, datetime_now

log = logging.getLogger(__name__)


class ToshiOpenquakeMeta(Model):
    """Stores metadata from the job configuration and the oq HDF5."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_WIP_OpenquakeMeta-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    partition_key = UnicodeAttribute(hash_key=True)  # a static value as we actually don't want to partition our data
    hazsol_vs30_rk = UnicodeAttribute(range_key=True)

    created = TimestampAttribute(default=datetime_now)

    hazard_solution_id = UnicodeAttribute()
    general_task_id = UnicodeAttribute()
    vs30 = NumberAttribute()  # vs30 value

    imts = UnicodeSetAttribute()  # list of IMTs
    locations_id = UnicodeAttribute()  # Location codes identifier (ENUM?)
    source_ids = UnicodeSetAttribute()
    source_tags = UnicodeSetAttribute()
    inv_time = NumberAttribute()  # Invesigation time in years

    # extracted from the OQ HDF5
    src_lt = JSONAttribute()  # sources meta as DataFrame JSON
    gsim_lt = JSONAttribute()  # gmpe meta as DataFrame JSON
    rlz_lt = JSONAttribute()  # realization meta as DataFrame JSON


class vs30_nloc1_gt_rlz_index(LocalSecondaryIndex):
    """
    Local secondary index with vs#) + 0.1 Degree search resolution
    """

    class Meta:
        # All attributes are projected
        projection = AllProjection()

    partition_key = UnicodeAttribute(hash_key=True)  # Same as the base table
    index1_rk = UnicodeAttribute(range_key=True)


class vs30_nloc001_gt_rlz_index(LocalSecondaryIndex):
    """
    Local secondary index with vs30:nloc_001:gtid:rlz6) 0.001 Degree search resolution
    """

    class Meta:
        # All attributes are projected
        projection = AllProjection()

    partition_key = UnicodeAttribute(hash_key=True)  # Same as the base table
    index2_rk = UnicodeAttribute(range_key=True)


class HazardAggregation(ModelCacheMixin, LocationIndexedModel):
    """A pynamodb model for aggregate hazard curves."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_HazardAggregation-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    hazard_model_id = UnicodeAttribute()
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)
    agg = EnumConstrainedUnicodeAttribute(AggregationEnum)

    values = ListAttribute(of=LevelValuePairAttribute)

    def set_location(self, location: CodedLocation):
        """Set internal fields, indices etc from the location."""
        super().set_location(location)

        # update the indices
        vs30s = str(self.vs30).zfill(VS30_KEYLEN)
        self.partition_key = self.nloc_1
        self.sort_key = f'{self.nloc_001}:{vs30s}:{self.imt}:{self.agg}:{self.hazard_model_id}'
        return self

    @staticmethod
    def to_csv(models: Iterable['HazardAggregation']) -> Iterator[Sequence[Union[str, float]]]:
        """Generate lists ready for csv module - including a header, followed by n rows."""
        n_models = 0
        for model in models:
            # create the header row, removing unneeded attributes
            if n_models == 0:
                model_attrs = list(model.attribute_values.keys())
                for attr in [
                    'hazard_model_id',
                    'uniq_id',
                    'created',
                    'nloc_0',
                    'nloc_001',
                    'nloc_01',
                    'nloc_1',
                    'partition_key',
                    'sort_key',
                    'values',
                ]:
                    model_attrs.remove(attr)

                levels = [f'poe-{value.lvl}' for value in model.values]
                yield (model_attrs + levels)

            # the data
            yield [getattr(model, attr) for attr in model_attrs] + [value.val for value in model.values]
            n_models += 1


class OpenquakeRealization(LocationIndexedModel):
    """Stores the individual hazard realisation curves."""

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_OpenquakeRealization-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    hazard_solution_id = UnicodeAttribute()
    source_tags = UnicodeSetAttribute()
    source_ids = UnicodeSetAttribute()

    rlz = IntegerAttribute()  # index of the openquake realization
    values = ListAttribute(of=IMTValuesAttribute)

    # Secondary Index attributes
    index1 = vs30_nloc1_gt_rlz_index()
    index1_rk = UnicodeAttribute()

    def set_location(self, location: CodedLocation):
        """Set internal fields, indices etc from the location."""
        super().set_location(location)

        # update the indices
        rlzs = str(self.rlz).zfill(6)

        vs30s = str(self.vs30).zfill(VS30_KEYLEN)
        self.partition_key = self.nloc_1
        self.sort_key = f'{self.nloc_001}:{vs30s}:{rlzs}:{self.hazard_solution_id}'
        self.index1_rk = f'{self.nloc_1}:{vs30s}:{rlzs}:{self.hazard_solution_id}'
        return self


tables = [
    OpenquakeRealization,
    ToshiOpenquakeMeta,
    HazardAggregation,
]


def migrate():
    """Create the tables, unless they exist already."""
    for table in tables:
        if not table.exists():  # pragma: no cover
            table.create_table(wait=True)
            log.info(f"Migrate created table: {table}")


def drop_tables():
    """Drop the tables, if they exist."""
    for table in tables:
        if table.exists():  # pragma: no cover
            table.delete_table()
            log.info(f'deleted table: {table}')
