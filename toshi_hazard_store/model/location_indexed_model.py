import uuid
from datetime import datetime, timezone

from nzshm_common.location.coded_location import CodedLocation
from pynamodb.attributes import UnicodeAttribute, VersionAttribute
from pynamodb.models import Model
from pynamodb_attributes import FloatAttribute, TimestampAttribute

from .attributes import EnumConstrainedIntegerAttribute
from .constraints import VS30Enum

VS30_KEYLEN = 3  # string length for VS30 field indices


def datetime_now():
    return datetime.now(tz=timezone.utc)


class LocationIndexedModel(Model):
    """Model base class."""

    __metaclass__ = type

    partition_key = UnicodeAttribute(hash_key=True)  # For this we will use a downsampled location to 1.0 degree
    sort_key = UnicodeAttribute(range_key=True)

    nloc_001 = UnicodeAttribute()  # 0.001deg ~100m grid
    nloc_01 = UnicodeAttribute()  # 0.01deg ~1km grid
    nloc_1 = UnicodeAttribute()  # 0.1deg ~10km grid
    nloc_0 = UnicodeAttribute()  # 1.0deg ~100km grid

    version = VersionAttribute()
    uniq_id = UnicodeAttribute()

    lat = FloatAttribute()  # latitude decimal degrees
    lon = FloatAttribute()  # longitude decimal degrees
    vs30 = EnumConstrainedIntegerAttribute(VS30Enum)
    site_vs30 = FloatAttribute(null=True)

    created = TimestampAttribute(default=datetime_now)

    def set_location(self, location: CodedLocation):
        """Set internal fields, indices etc from the location."""

        self.nloc_001 = location.downsample(0.001).code
        self.nloc_01 = location.downsample(0.01).code
        self.nloc_1 = location.downsample(0.1).code
        self.nloc_0 = location.downsample(1.0).code
        # self.nloc_10 = location.downsample(10.0).code

        self.lat = location.lat
        self.lon = location.lon
        self.uniq_id = str(uuid.uuid4())
        return self
