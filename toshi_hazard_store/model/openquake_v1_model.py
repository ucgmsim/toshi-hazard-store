"""This module defines the pynamodb tables used to store openquake data."""

from pynamodb.attributes import MapAttribute, NumberAttribute


class LevelValuePairAttribute(MapAttribute):
    """Store the IMT level and the POE value at the level."""

    lvl = NumberAttribute(null=False)
    val = NumberAttribute(null=False)
