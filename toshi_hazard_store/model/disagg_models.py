"""This module defines the pynamodb tables used to store openquake data. Third iteration"""

import logging
from datetime import datetime, timezone

import numpy as np
from nzshm_common.location.code_location import CodedLocation
from pynamodb.attributes import UnicodeAttribute
from pynamodb_attributes import FloatAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

from .attributes import CompressedPickleAttribute, EnumAttribute, EnumConstrainedUnicodeAttribute, PickleAttribute
from .constraints import AggregationEnum, IntensityMeasureTypeEnum, ProbabilityEnum
from .location_indexed_model import VS30_KEYLEN, LocationIndexedModel


def datetime_now():
    return datetime.now(tz=timezone.utc)


log = logging.getLogger(__name__)


class DisaggAggregationBase(LocationIndexedModel):
    """Store aggregated disaggregations."""

    hazard_model_id = UnicodeAttribute()
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)

    hazard_agg = EnumConstrainedUnicodeAttribute(AggregationEnum)  # eg MEAN
    disagg_agg = EnumConstrainedUnicodeAttribute(AggregationEnum)

    disaggs = CompressedPickleAttribute()  # a very compressible numpy array,
    bins = PickleAttribute()  # a much smaller numpy array

    shaking_level = FloatAttribute()

    probability = EnumAttribute(ProbabilityEnum)  # eg TEN_PCT_IN_50YRS

    def set_location(self, location: CodedLocation):
        """Set internal fields, indices etc from the location."""
        super().set_location(location)

        # update the indices
        vs30s = str(self.vs30).zfill(VS30_KEYLEN)
        self.partition_key = self.nloc_1
        self.sort_key = (
            f'{self.hazard_model_id}:{self.hazard_agg}:{self.disagg_agg}:'
            f'{self.nloc_001}:{vs30s}:{self.imt}:{self.probability.name}'
        )
        return self


class DisaggAggregationExceedance(DisaggAggregationBase):
    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_DisaggAggregationExceedance-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    @staticmethod
    def new_model(
        hazard_model_id: str,
        location: CodedLocation,
        vs30: str,
        imt: str,
        hazard_agg: AggregationEnum,
        disagg_agg: AggregationEnum,
        probability: ProbabilityEnum,
        shaking_level: float,
        disaggs: np.ndarray,
        bins: np.ndarray,
    ) -> 'DisaggAggregationExceedance':
        obj = DisaggAggregationExceedance(
            hazard_model_id=hazard_model_id,
            vs30=vs30,
            imt=imt,
            hazard_agg=hazard_agg,
            disagg_agg=disagg_agg,
            probability=probability,
            shaking_level=shaking_level,
            disaggs=disaggs,
            bins=bins,
        )
        obj.set_location(location)
        return obj


class DisaggAggregationOccurence(DisaggAggregationBase):
    class Meta:
        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_DisaggAggregationOccurence-{DEPLOYMENT_STAGE}"
        region = REGION

        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover


tables = [
    DisaggAggregationExceedance,
    DisaggAggregationOccurence,
]


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
