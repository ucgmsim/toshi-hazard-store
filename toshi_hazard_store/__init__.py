"""Top-level package for toshi-hazard-store."""

__author__ = """GNS Science"""
__email__ = 'chrisbc@artisan.co.nz'
__version__ = '0.8.0'


import toshi_hazard_store.model as model
import toshi_hazard_store.query.hazard_query as query_v3  # alias for clients using deprecated module name
from toshi_hazard_store.config import USE_SQLITE_ADAPTER
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter
from toshi_hazard_store.model import configure_adapter

if USE_SQLITE_ADAPTER:
    configure_adapter(adapter_model=SqliteAdapter)
