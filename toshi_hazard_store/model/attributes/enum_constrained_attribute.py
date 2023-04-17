"""This module defines some custom enum attributes."""
import logging
from enum import Enum
from typing import Any, Optional, Type, TypeVar, Union

from pynamodb.attributes import Attribute
from pynamodb.constants import NUMBER, STRING

T = TypeVar("T", bound=Enum)
_T = TypeVar('_T')

log = logging.getLogger(__name__)


class EnumConstrainedAttributeMixin:

    attr_type = STRING
    value_type = Any
    enum_type: Any

    def _validate_enum(self, enum_type, *args):
        if not all(isinstance(e.value, self.value_type) for e in self.enum_type):
            raise TypeError(
                f"Enumeration '{self.enum_type}' values must be all {self.value_type}",
            )

    def deserialize(self, value: Any) -> Union[str, float, int]:
        try:
            assert self.enum_type(value)
            return value
        except (ValueError):
            raise ValueError(f'value {value} must be a member of {self.enum_type}')

    def serialize(self, value: Any) -> str:
        try:
            if isinstance(value, self.enum_type):
                log.info(f'user passed in enum type {value} {str(value.value)}')  # type: ignore
                return str(value.value)
            # if not isinstance(value, self.value_type):
            #     raise ValueError(f'value {value} must be a member of {self.enum_type}')
            self.enum_type(value)
        except (ValueError) as err:
            raise err
        return str(value)


class EnumConstrainedUnicodeAttribute(EnumConstrainedAttributeMixin, Attribute[T]):
    """
    Stores values of the supplied Unicode Enum as DynamoDB STRING types.

    Useful where you have values in an existing table field and you want retrofit Enum validation.

    >>> from enum import Enum
    >>> from pynamodb.models import Model
    >>>
    >>> class ShakeFlavor(Enum):
    >>>   VANILLA = 'vanilla'
    >>>   MINT = 'mint'
    >>>
    >>> class Shake(Model):
    >>>   flavor = EnumConstrainedAttribute(ShakeFlavor)
    >>>
    >>> modelB = Shake(flavor='mint')
    >>>
    """

    attr_type = STRING
    value_type = str

    def __set__(self, instance: Any, value: Optional[T]) -> None:
        if isinstance(value, self.enum_type):
            log.info(f'user __set__ enum type {value} {str(value.value)}')
            super().__set__(instance, value.value)
        else:
            super().__set__(instance, value)

    def __init__(self, enum_type: Type[T], **kwargs: Any) -> None:
        """
        :param enum_type: The type of the enum
        """
        super().__init__(**kwargs)
        self.enum_type = enum_type
        super()._validate_enum(self.enum_type, self.value_type)

    def deserialize(self, value: Union[float, int]) -> str:
        return str(super().deserialize(value))

    def serialize(self, value: Union[float, int]) -> str:
        return super().serialize(value)


class EnumConstrainedIntegerAttribute(EnumConstrainedAttributeMixin, Attribute[T]):
    attr_type = NUMBER
    value_type = int

    def __init__(self, enum_type: Type[T], **kwargs: Any) -> None:
        """
        :param enum_type: The type of the enum
        """
        super().__init__(**kwargs)
        self.enum_type = enum_type
        super()._validate_enum(self.enum_type, self.value_type)

    def __set__(self, instance: Any, value: Optional[T]) -> None:
        if isinstance(value, self.enum_type):
            log.info(f'user __set__ enum type {value} {str(value.value)}')
            super().__set__(instance, value.value)
        else:
            super().__set__(instance, value)

    def deserialize(self, value: Union[float, int]) -> int:
        return int(super().deserialize(int(value)))

    def serialize(self, value: Union[float, int]) -> str:
        return super().serialize(int(value))


class EnumConstrainedFloatAttribute(EnumConstrainedAttributeMixin, Attribute[T]):
    attr_type = NUMBER
    value_type = float

    def __init__(self, enum_type: Type[T], **kwargs: Any) -> None:
        """
        :param enum_type: The type of the enum
        """
        super().__init__(**kwargs)
        self.enum_type = enum_type
        super()._validate_enum(self.enum_type, self.value_type)

    def __set__(self, instance: Any, value: Optional[T]) -> None:
        if isinstance(value, self.enum_type):
            log.info(f'user __set__ enum type {value} {str(value.value)}')
            super().__set__(instance, value.value)
        else:
            super().__set__(instance, value)

    def deserialize(self, value: Union[float, int]) -> float:
        return float(super().deserialize(float(value)))

    def serialize(self, value: Union[float, int]) -> str:
        return super().serialize(float(value))
