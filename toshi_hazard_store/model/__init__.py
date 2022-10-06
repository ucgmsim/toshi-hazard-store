from .gridded_hazard import CompressedListAttribute, GriddedHazard
from .gridded_hazard import drop_tables as drop_gridded
from .gridded_hazard import migrate as migrate_gridded
from .openquake_v1_model import LevelValuePairAttribute
from .openquake_v2_model import IMTValuesAttribute
from .openquake_v3_model import VS30_KEYLEN, HazardAggregation, OpenquakeRealization, ToshiOpenquakeMeta
from .openquake_v3_model import drop_tables as drop_tables_v3
from .openquake_v3_model import migrate as migrate_v3
from .openquake_v3_model import tables as oqv3_tables
from .openquake_v3_model import vs30_nloc001_gt_rlz_index


def migrate():
    """Create the tables, unless they exist already."""
    migrate_v3()
    migrate_gridded()


def drop_tables():
    """Drop em"""
    drop_tables_v3()
    drop_gridded()
