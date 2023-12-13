"""
An adapter class that overrides the standard pynamodb operations so that
they can be supplied via a suitable adapter implementaion

 - query
 - create_table
 - delete_table

"""
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Optional, Type, TypeVar

import pynamodb.models
from pynamodb.connection.base import OperationSettings
from pynamodb.expressions.condition import Condition

log = logging.getLogger(__name__)

_T = TypeVar('_T', bound='pynamodb.models.Model')
_KeyType = Any


class PynamodbAdapterInterface(ABC):
    """
    Defines methods to be provided by a adapter class implementation.
    """

    @abstractmethod
    def get_table_connector(model_class: Type[_T]):
        """get a connector to the storage table"""
        pass
    
    def drop_table(model_class: Type[_T]):
        pass

    def get_model(connection, range_key_condition, filter_condition):
        """Get iterator for given conditions"""
        pass
    
    def put_model(conn, item):
        """Put an item to the store"""
        pass

    def drop_model(conn, item):
        """Put and item to the store"""
        pass

    def drop_model(conn, res):
        """Put and item to the store"""
        pass

    def count_hits(filter_condition):
        """Count minimum"""
        pass


class ModelAdapterMixin(pynamodb.models.Model):
    """extends pynamodb.models.Model with a pluggable model."""

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
    ) -> pynamodb.models.ResultIterator[_T]:  #
        """
        Proxy query function which trys to use the local_cache before hitting AWS via Pynamodb
        """

        # CBC TODO support optional filter condition if supplied range_condition operand is "="
        if (not cache_store.cache_enabled()) and (filter_condition is not None):
            log.warning("Not using the cache")
            return super().query(  # type: ignore
                hash_key,
                range_key_condition,
                filter_condition,
                consistent_read,
                index_name,
                scan_index_forward,
                limit,
                last_evaluated_key,
                attributes_to_get,
                page_size,
                rate_limit,
                settings,
            )

        log.info('Try the local_cache first')

        if isinstance(filter_condition, Condition):
            conn = cache_store.get_connection(model_class=cls)
            cached_rows = list(cache_store.get_model(conn, cls, range_key_condition, filter_condition))  # type: ignore

            minimum_expected_hits = cache_store.count_permutations(filter_condition)
            log.info('permutations: %s cached_rows: %s' % (minimum_expected_hits, len(cached_rows)))

            if len(cached_rows) >= minimum_expected_hits:
                return cached_rows  # type: ignore
            if len(cached_rows) < minimum_expected_hits:
                log.warn('permutations: %s cached_rows: %s' % (minimum_expected_hits, len(cached_rows)))
                result = []
                for res in super().query(  # type: ignore
                    hash_key,
                    range_key_condition,
                    filter_condition,
                    consistent_read,
                    index_name,
                    scan_index_forward,
                    limit,
                    last_evaluated_key,
                    attributes_to_get,
                    page_size,
                    rate_limit,
                    settings,
                ):
                    cache_store.put_model(conn, res)
                    result.append(res)
                return result  # type: ignore

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
        cache_store = cls.AdapterMeta.adapter
        
        if cache_store.cache_enabled():
            log.info("setup local cache")
            conn = cache_store.get_connection(model_class=cls)
            cache_store.ensure_table_exists(conn, model_class=cls)

        return super().create_table(  # type: ignore
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
        return super().delete_table()  # type: ignore
