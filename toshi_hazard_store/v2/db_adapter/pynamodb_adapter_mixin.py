"""
An adapter class that overrides the standard pynamodb operations so that
they can be supplied via a suitable adapter implementaion

 - query
 - create_table
 - delete_table

"""
import logging
from typing import Any, Dict, Iterable, Optional, Type, TypeVar

import pynamodb.models
from pynamodb.connection.base import OperationSettings
from pynamodb.expressions.condition import Condition

log = logging.getLogger(__name__)

_T = TypeVar('_T', bound='pynamodb.models.Model')
_KeyType = Any


class ModelAdapterMixin(pynamodb.models.Model):
    """extends pynamodb.models.Model with a pluggable model."""

    def save(self):
        raise NotImplementedError()

    @classmethod
    def exists(
        cls: Type[_T],
    ):
        adapter = cls.AdapterMeta.adapter  # type: ignore
        conn = adapter.get_connection()
        return adapter.exists(conn, cls)
        raise NotImplementedError()

    @classmethod
    def query(
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
    ) -> pynamodb.models.ResultIterator[_T]:  #
        adapter = cls.AdapterMeta.adapter  # type: ignore
        conn = adapter.get_connection()
        return adapter.get_model(conn, cls, hash_key, range_key_condition, filter_condition)

    @classmethod
    def create_table(
        cls: Type[_T],
        wait: bool = False,
        read_capacity_units: Optional[int] = None,
        write_capacity_units: Optional[int] = None,
        billing_mode: Optional[str] = None,
        ignore_update_ttl_errors: bool = False,
    ):
        """
        extends create_table to manage the local_cache table.
        """
        adapter = cls.AdapterMeta.adapter  # type: ignore
        conn = adapter.get_connection()
        return adapter.create_table(
            conn,
            cls,
            wait,
            read_capacity_units,
            write_capacity_units,
            billing_mode,
            ignore_update_ttl_errors,
        )

    @classmethod
    def delete_table(cls: Type[_T]):
        """
        extends delete_table to manage the local_cache table.
        """
        log.info('drop the table ')
        adapter = cls.AdapterMeta.adapter  # type: ignore
        conn = adapter.get_connection()
        return adapter.delete_table(conn, cls)
