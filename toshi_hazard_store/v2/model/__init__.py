
from .openquake_models import ToshiV2DemoTable

from .openquake_models import drop_tables as drop_openquake
from .openquake_models import migrate as migrate_openquake

def migrate():
    """Create the tables, unless they exist already."""
    openquake_models.migrate()

def drop_tables():
    """Drop em"""
    openquake_models.drop_tables()
