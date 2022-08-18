"""This module defines the pynamodb tables used to store openquake v2 data."""

from pynamodb.attributes import ListAttribute, MapAttribute, NumberAttribute, UnicodeAttribute


class IMTValuesAttribute(MapAttribute):
    """Store the IntensityMeasureType e.g.(PGA, SA(N)) and the levels and values lists."""

    imt = UnicodeAttribute()
    lvls = ListAttribute(of=NumberAttribute)
    vals = ListAttribute(of=NumberAttribute)
