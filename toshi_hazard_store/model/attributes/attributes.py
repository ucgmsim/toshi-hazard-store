"""This module defines some custom attributes."""

import json
import pickle
import zlib
from typing import Any, Dict, List, Tuple, Union

from nzshm_common.util import compress_string, decompress_string
from pynamodb.attributes import (
    Attribute,
    BinaryAttribute,
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    UnicodeAttribute,
)
from pynamodb.constants import BINARY, STRING


class ForeignKeyAttribute(UnicodeAttribute):
    """
    A string representation of a (hash_key, range_key) tuple.
    """

    def serialize(self, value: Tuple[str, str]) -> str:
        # print(value)
        assert len(value) == 2
        return super().serialize("_".join(value))

    def deserialize(self, value: str) -> Tuple[str, str]:
        tup = super().deserialize(value).split("_")
        if not len(tup) == 2:
            raise ValueError(f"Invalid value cannot be deserialised: {value}")
        return tuple(tup)


class IMTValuesAttribute(MapAttribute):
    """Store the IntensityMeasureType e.g.(PGA, SA(N)) and the levels and values lists."""

    imt = UnicodeAttribute()
    lvls = ListAttribute(of=NumberAttribute)
    vals = ListAttribute(of=NumberAttribute)


class LevelValuePairAttribute(MapAttribute):
    """Store the IMT level and the POE value at the level."""

    lvl = NumberAttribute(null=False)
    val = NumberAttribute(null=False)


class CompressedJsonicAttribute(Attribute):
    """
    A compressed, json serialisable model attribute
    """

    attr_type = STRING

    def serialize(self, value: Any) -> str:
        return compress_string(json.dumps(value))  # could this be pickle??

    def deserialize(self, value: str) -> Union[Dict, List]:
        return json.loads(decompress_string(value))


class CompressedListAttribute(CompressedJsonicAttribute):
    """
    A compressed list of floats attribute.
    """

    def serialize(self, value: List[float]) -> str:
        # value = list(value)
        if value is not None and not isinstance(value, list):
            raise TypeError(
                f"value has invalid type '{type(value)}'; List[float])expected",
            )
        return super().serialize(value)


class CompressedPickleAttribute(Attribute[bytes]):
    """
    An attribute containing a binary data object (:code:`bytes`)
    """

    attr_type = BINARY

    def serialize(self, value: bytes):
        return zlib.compress(pickle.dumps(value))

    def deserialize(self, value: bytes):
        return pickle.loads(zlib.decompress(value))


class PickleAttribute(BinaryAttribute):
    """
    This class will serialize/deserialize any picklable Python object.
    """

    legacy_encoding = True

    def serialize(self, value):
        """
        The super class takes the binary string returned from pickle.dumps
        and encodes it for storage in DynamoDB
        """
        return super(PickleAttribute, self).serialize(pickle.dumps(value))

    def deserialize(self, value):
        return pickle.loads(super(PickleAttribute, self).deserialize(value))
