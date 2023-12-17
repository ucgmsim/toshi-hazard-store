from .openquake_models import ToshiV2DemoTable, OpenquakeRealization, ToshiOpenquakeMeta, HazardAggregation
from .openquake_models import drop_tables as drop_openquake
from .openquake_models import migrate as migrate_openquake

from ...model.attributes.attributes import IMTValuesAttribute, LevelValuePairAttribute

def migrate():
    """Create the tables, unless they exist already."""
    migrate_openquake()


def drop_tables():
    """Drop em"""
    drop_openquake()
