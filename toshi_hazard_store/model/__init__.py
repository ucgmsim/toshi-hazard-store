import logging
from typing import Type

from toshi_hazard_store.db_adapter import PynamodbAdapterInterface, ensure_class_bases_begin_with

from . import location_indexed_model, openquake_models, revision_4
from .attributes import IMTValuesAttribute, LevelValuePairAttribute
from .constraints import AggregationEnum, IntensityMeasureTypeEnum, ProbabilityEnum, VS30Enum
from .disagg_models import DisaggAggregationExceedance, DisaggAggregationOccurence
from .disagg_models import drop_tables as drop_disagg
from .disagg_models import migrate as migrate_disagg
from .gridded_hazard import GriddedHazard
from .gridded_hazard import drop_tables as drop_gridded
from .gridded_hazard import migrate as migrate_gridded
from .location_indexed_model import LocationIndexedModel
from .openquake_models import VS30_KEYLEN, HazardAggregation, OpenquakeRealization, ToshiOpenquakeMeta
from .openquake_models import drop_tables as drop_openquake
from .openquake_models import migrate as migrate_openquake
from .openquake_models import vs30_nloc001_gt_rlz_index
from .revision_4 import (  # , HazardRealizationMeta
    CompatibleHazardCalculation,
    HazardAggregateCurve,
    HazardCurveProducerConfig,
    HazardRealizationCurve,
)
from .revision_4 import drop_tables as drop_r4
from .revision_4 import migrate as migrate_r4

# from .openquake_models import tables as oqv3_tables
# from .openquake_v2_model import

log = logging.getLogger(__name__)


def migrate():
    """Create the tables, unless they exist already."""
    migrate_openquake()
    migrate_gridded()
    migrate_disagg()


def drop_tables():
    """Drop em"""
    drop_openquake()
    drop_gridded()
    drop_disagg()


def configure_adapter(adapter_model: Type[PynamodbAdapterInterface]):
    log.info(f"Configure adapter: {adapter_model}")
    ensure_class_bases_begin_with(
        namespace=openquake_models.__dict__,
        class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
        base_class=adapter_model,
    )
    ensure_class_bases_begin_with(
        namespace=location_indexed_model.__dict__, class_name=str('LocationIndexedModel'), base_class=adapter_model
    )
    ensure_class_bases_begin_with(
        namespace=openquake_models.__dict__,
        class_name=str('OpenquakeRealization'),  # `str` type differs on Python 2 vs. 3.
        base_class=adapter_model,
    )
    ensure_class_bases_begin_with(
        namespace=openquake_models.__dict__,
        class_name=str('HazardAggregation'),
        base_class=adapter_model,
    )

    ### New Rev 4 tables
    ensure_class_bases_begin_with(
        namespace=revision_4.hazard_realization_curve.__dict__,
        class_name=str('HazardRealizationCurve'),
        base_class=adapter_model,
    )
    ensure_class_bases_begin_with(
        namespace=revision_4.hazard_models.__dict__,
        class_name=str('HazardCurveProducerConfig'),
        base_class=adapter_model,
    )
    ensure_class_bases_begin_with(
        namespace=revision_4.hazard_models.__dict__,
        class_name=str('CompatibleHazardCalculation'),
        base_class=adapter_model,
    )
    ensure_class_bases_begin_with(
        namespace=revision_4.hazard_aggregate_curve.__dict__,
        class_name=str('HazardAggregateCurve'),
        base_class=adapter_model,
    )
