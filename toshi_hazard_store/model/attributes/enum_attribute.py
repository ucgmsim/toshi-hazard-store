"""This module defines a custom enum attribute."""
import logging
from enum import Enum
from typing import Any, Type, TypeVar

from pynamodb.attributes import Attribute
from pynamodb.constants import STRING

T = TypeVar("T", bound=Enum)
_T = TypeVar('_T')

log = logging.getLogger(__name__)


class EnumAttribute(Attribute[T]):
    """
    Stores names of the supplied Enum as DynamoDB strings.

    >>> from enum import Enum
    >>>
    >>> from pynamodb.models import Model
    >>>
    >>> class ShakeFlavor(Enum):
    >>>   VANILLA = 0.1
    >>>   MINT = 1.22
    >>>
    >>> class Shake(Model):
    >>>   flavor = EnumAttribute(ShakeFlavor)
    >>>
    >>> modelB = Shake(flavor=ShakeFlavor.MINT)
    >>> assert modelB.flavor == ShakeFlavor.MINT
    """

    attr_type = STRING

    def __init__(self, enum_type: Type[T], **kwargs: Any) -> None:
        """
        :param enum_type: The type of the enum
        """
        super().__init__(**kwargs)
        self.enum_type = enum_type

    def deserialize(self, value: str) -> Type[T]:
        log.info(f'user deserialize value {value}')
        try:
            val = self.enum_type[value]  # getattr(self.enum_type, value)
            log.info(f'enum: {val}')
            # return val
            return super().deserialize(val)
        except (AttributeError, KeyError):
            raise ValueError(f'stored value {value} must be a member of  {self.enum_type}.')

    def serialize(self, value: Type[T]) -> str:
        log.info(f'user serialize value {value}')
        if isinstance(value, self.enum_type):
            print(f'serialize value {value}')
            return super().serialize(value.name)
        else:
            try:
                assert self.enum_type(value)  # CBC MARKS
                return super().serialize(value)
            except (Exception) as err:
                print(err)
                raise ValueError(f'value {value} must be a member of {self.enum_type}.')
