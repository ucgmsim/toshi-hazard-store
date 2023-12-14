"""
Implement db adapter for slqlite
"""
import logging
import pathlib
import sqlite3
from typing import TYPE_CHECKING, Any, Iterable, Type, TypeVar, Union

from pynamodb.expressions.condition import Condition

from toshi_hazard_store.model.caching.cache_store import check_exists, drop_table, ensure_table_exists, put_model

from .pynamodb_adapter_interface import PynamodbAdapterInterface

if TYPE_CHECKING:
    import pynamodb.models.Model

_T = TypeVar('_T', bound='pynamodb.models.Model')
_KeyType = Any

LOCAL_STORAGE_FOLDER = "/GNSDATA/API/toshi-hazard-store/LOCALSTORAGE"
DEPLOYMENT_STAGE = "DEV"

log = logging.getLogger(__name__)


class SqliteAdapter(PynamodbAdapterInterface):
    def get_connection(self) -> sqlite3.Connection:
        dbpath = pathlib.Path(str(LOCAL_STORAGE_FOLDER), DEPLOYMENT_STAGE, 'model.db')
        assert dbpath.parent.exists()
        log.info(f"get sqlite3 connection at {dbpath}")
        return sqlite3.connect(dbpath)

    @staticmethod
    def save(connection: Any, model_instance: Any) -> None:  # sqlite3.Connection
        return put_model(connection, model_instance)

    @staticmethod
    def exists(connection: Any, model_class: Type[_T]):
        return check_exists(connection, model_class)

    @staticmethod
    def create_table(connection: Any, model_class: Type[_T], *args, **kwargs):
        dynamodb_defaults = dict(  # noqa
            wait=False,
            read_capacity_units=None,
            write_capacity_units=None,
            billing_mode=None,
            ignore_update_ttl_errors=False,
        )
        return ensure_table_exists(connection, model_class)

    @staticmethod
    def delete_table(connection: Any, model_class: Type[_T]):
        return drop_table(connection, model_class)

    @staticmethod
    def get_model(
        connection: Any,  # sqlite3.Connection
        model_class: Type[_T],
        hash_key: str,  # CompulsoryHashKey
        range_key_condition: Condition,
        filter_condition: Union[Condition, None] = None,
    ) -> Iterable[_T]:
        """query cache table and return any hits.
        :param conn: Connection object
        :param model_class: type of the model_class
        :return:
        """
        raise NotImplementedError()
        # return get_model(connection, model_class, range_key_condition, filter_condition)

    @staticmethod
    def count_hits(filter_condition):
        """Count minimum"""
        raise NotImplementedError()
