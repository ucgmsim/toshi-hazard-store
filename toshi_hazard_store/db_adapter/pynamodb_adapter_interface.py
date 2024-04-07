"""
Defines methods to be provided by a adapter class implementation.

The intention is that concrete adapter implementations must adhere to the
Model API from PynamoDB.

For details of how this works
 - https://mypy.readthedocs.io/en/stable/metaclasses.html#gotchas-and-limitations-of-metaclass-support
 - https://stackoverflow.com/a/76681565

"""

from abc import ABC, ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Type, TypeVar

from pynamodb.models import Condition, MetaModel, Model
from pynamodb.pagination import ResultIterator

if TYPE_CHECKING:
    import pynamodb.models.Model

_T = TypeVar(
    '_T', bound='pynamodb.models.Model'
)  # TODO figure out how to extend the pynamodb Model with the AdapterMeta attribute


class _ABCModelMeta(MetaModel, ABCMeta):
    """Combine the metaclasses needed for the interface base class"""


class ABCModel(Model, ABC, metaclass=_ABCModelMeta):
    """A base class with the superclasses `Model` & `ABC`"""


# cant' use this yet, see https://stackoverflow.com/questions/11276037/resolving-metaclass-conflicts/61350480#61350480
class PynamodbAdapterInterface(ABCModel):
    """
    Defines the interface for concrete adapter implementations.
    """

    @classmethod
    @abstractmethod
    def create_table(model_class: Type[_T], *args, **kwargs):
        pass

    @classmethod
    @abstractmethod
    def delete_table(model_class: Type[_T]):
        pass

    @classmethod
    @abstractmethod
    def query(
        model_class: Type[_T],
        hash_key: Any,
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
    ) -> ResultIterator['PynamodbAdapterInterface']:
        """Get iterator for given conditions"""
        pass

    @classmethod
    @abstractmethod
    def count(
        model_class: Type[_T],
        hash_key: Optional[Any] = None,
        range_key_condition: Optional[Condition] = None,
        filter_condition: Optional[Condition] = None,
        consistent_read: bool = False,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        rate_limit: Optional[float] = None,
    ) -> int:
        """Get iterator for given conditions"""
        pass

    @abstractmethod
    def save(self: _T, *args, **kwargs) -> dict[str, Any]:
        """Put an item to the store"""
        pass

    def drop_model(connection, res):
        """Put and item to the store"""
        pass

    def count_hits(filter_condition):
        """Count minimum"""
        pass
