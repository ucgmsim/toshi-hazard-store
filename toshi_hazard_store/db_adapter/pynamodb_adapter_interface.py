"""
Defines methods to be provided by a adapter class implementation.
"""
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Type, TypeVar

if TYPE_CHECKING:
    import pynamodb.models.Model

_T = TypeVar(
    '_T', bound='pynamodb.models.Model'
)  # TODO figure out how to extend the pynamodb Model with the AdapterMeta attribute
_KeyType = Any


# cant' use this yet, see https://stackoverflow.com/questions/11276037/resolving-metaclass-conflicts/61350480#61350480
class PynamodbAdapterInterface(ABC):
    """
    Defines methods to be provided by a adapter class implementation.
    """

    @abstractmethod
    def get_connection(self, model_class: Type[_T]):
        """get a connector to the storage engine"""
        pass

    @staticmethod
    @abstractmethod
    def create_table(connection: Any, model_class: Type[_T], *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def delete_table(connection: Any, model_class: Type[_T]):
        pass

    @staticmethod
    @abstractmethod
    def query(connection: Any, model_class: Type[_T], hash_key: str, range_key_condition, filter_condition):
        """Get iterator for given conditions"""
        pass

    @staticmethod
    @abstractmethod
    def save(connection: Any, model_instance: _T) -> None:
        """Put an item to the store"""
        pass

    def drop_model(connection, res):
        """Put and item to the store"""
        pass

    def count_hits(filter_condition):
        """Count minimum"""
        pass
