"""
Implement db adapter for slqlite
"""
import logging
import pathlib
import sqlite3
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type, TypeVar

import pynamodb.models
from pynamodb.connection.base import OperationSettings
from pynamodb.expressions.condition import Condition

from ..pynamodb_adapter_interface import PynamodbAdapterInterface  # noqa
from .sqlite_store import check_exists, drop_table, ensure_table_exists, get_model, put_model, safe_table_name

if TYPE_CHECKING:
    import pynamodb.models.Model

_T = TypeVar('_T', bound='pynamodb.models.Model')
_KeyType = Any

LOCAL_STORAGE_FOLDER = "/GNSDATA/API/toshi-hazard-store/LOCALSTORAGE"
DEPLOYMENT_STAGE = "DEV"

log = logging.getLogger(__name__)


def get_connection(model_class: Type[_T]) -> sqlite3.Connection:
    dbpath = pathlib.Path(LOCAL_STORAGE_FOLDER) / DEPLOYMENT_STAGE / f"{safe_table_name(model_class)}.db"
    assert dbpath.parent.exists()
    log.debug(f"get sqlite3 connection at {dbpath}")
    return sqlite3.connect(dbpath)


# see https://stackoverflow.com/questions/11276037/resolving-metaclass-conflicts/61350480#61350480
class SqliteAdapter(pynamodb.models.Model):  # PynamodbAdapterInterface):

    adapted_model = sqlite3

    def save(
        self: _T,
        condition: Optional[Condition] = None,
        settings: OperationSettings = OperationSettings.default,
        add_version_condition: bool = False,
    ) -> dict[str, Any]:
        return put_model(get_connection(type(self)), self)

    # def save(self: _T) -> None:
    #     return put_model(get_connection(type(self)), self)

    @classmethod
    def exists(cls: Type[_T]) -> bool:
        return check_exists(get_connection(cls), cls)

    @classmethod
    def create_table(
        cls: Type[_T],
        wait: bool = False,
        read_capacity_units: Optional[int] = None,
        write_capacity_units: Optional[int] = None,
        billing_mode: Optional[str] = None,
        ignore_update_ttl_errors: bool = False,
    ):
        return ensure_table_exists(get_connection(cls), cls)

    @classmethod
    def delete_table(cls: Type[_T]):
        return drop_table(get_connection(cls), cls)

    @classmethod
    def query(  # type: ignore
        cls: Type[_T],
        hash_key: _KeyType,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        scan_index_forward: Optional[bool] = None,
        limit: Optional[int] = None,
        last_evaluated_key: Optional[Dict[str, Dict[str, Any]]] = None,
        attributes_to_get: Optional[Iterable[str]] = None,
        page_size: Optional[int] = None,
        rate_limit: Optional[float] = None,
        settings: OperationSettings = OperationSettings.default,
    ) -> Iterable[_T]:  #
        return get_model(get_connection(cls), cls, hash_key, range_key_condition, filter_condition)

    @staticmethod
    def count_hits(filter_condition):
        """Count minimum"""
        raise NotImplementedError()
