"""
Implement db adapter for slqlite
"""

import logging
import pathlib
import sqlite3
from typing import TYPE_CHECKING, Any, Dict, Generic, Iterable, List, Optional, Type, TypeVar

import pynamodb.models
from pynamodb.constants import DELETE, PUT
from pynamodb.expressions.condition import Condition

from toshi_hazard_store.config import SQLITE_ADAPTER_FOLDER

from ..pynamodb_adapter_interface import PynamodbAdapterInterface  # noqa
from .pynamodb_sql import get_version_attribute
from .sqlite_store import (
    check_exists,
    count_model,
    drop_table,
    ensure_table_exists,
    get_model,
    put_model,
    put_models,
    safe_table_name,
)

if TYPE_CHECKING:
    import pynamodb.models.Model

_T = TypeVar('_T', bound='pynamodb.models.Model')
_KeyType = Any

BATCH_WRITE_PAGE_LIMIT = 250

log = logging.getLogger(__name__)


def get_connection(model_class: Type[_T]) -> sqlite3.Connection:
    if not SQLITE_ADAPTER_FOLDER:
        raise RuntimeError('Environment variable: THS_SQLITE_FOLDER is not set.')
    dbpath = pathlib.Path(SQLITE_ADAPTER_FOLDER) / f"{safe_table_name(model_class)}.db"
    if not dbpath.parent.exists():
        raise RuntimeError(f'The sqlite storage folder "{dbpath.parent.absolute()}" was not found.')
    log.debug(f"get sqlite3 connection at {dbpath}")
    return sqlite3.connect(dbpath)


class SqliteBatchWrite(pynamodb.models.BatchWrite, Generic[_T]):
    def __init__(self, model: Type[_T], auto_commit: bool = True):
        super().__init__(model, auto_commit)
        self.max_operations = BATCH_WRITE_PAGE_LIMIT

    def commit(self) -> None:
        """
        Writes all of the changes that are pending
        """
        log.debug("%s committing batch operation", self.model)
        put_items: List[_T] = []
        delete_items: List[_T] = []
        for item in self.pending_operations:
            if item['action'] == PUT:
                put_items.append(item['item'])
            elif item['action'] == DELETE:
                raise NotImplementedError("Batch delete not implemented")
                delete_items.append(item['item']._get_keys())
        self.pending_operations = []

        if not len(put_items) and not len(delete_items):
            return

        return put_models(
            get_connection(self.model),
            put_items=put_items,
            # delete_items=delete_items
        )


class SqliteAdapter(PynamodbAdapterInterface):
    @classmethod
    def batch_write(
        cls: Type[_T],
        auto_commit: bool = True,
    ) -> SqliteBatchWrite[_T]:
        """
        Returns a BatchWrite context manager for a batch operation.
        """
        return SqliteBatchWrite(cls, auto_commit=auto_commit)

    def save(
        self: _T,
        condition: Optional[Condition] = None,
        add_version_condition: bool = False,
    ) -> dict[str, Any]:
        log.debug('SqliteAdapter.save')

        version_attr = get_version_attribute(self)
        if version_attr:
            # simple_serialized = self.to_simple_dict(force=True)
            value = getattr(self, version_attr.attr_name)
            # value = simple_serialized.get(version_attr.attr_name)
            if not value:
                setattr(self, version_attr.attr_name, 1)
            else:
                setattr(self, version_attr.attr_name, value + 1)
        return put_model(get_connection(type(self)), self)

    @classmethod
    def exists(cls: Type[_T]) -> bool:
        """Override pynamodb exits()for sqlite"""
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
    ) -> Iterable[_T]:  #
        if range_key_condition is None:
            raise TypeError("must supply range_key_condition argument")
        return get_model(get_connection(cls), cls, hash_key, range_key_condition, filter_condition)

    @classmethod
    def count(
        cls: Type[_T],
        hash_key: Optional[Any] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
    ) -> int:
        if range_key_condition is None:
            raise TypeError("must supply range_key_condition argument")
        return count_model(get_connection(cls), cls, hash_key, range_key_condition, filter_condition)

    @staticmethod
    def count_hits(filter_condition):
        """Count minimum"""
        raise NotImplementedError()
