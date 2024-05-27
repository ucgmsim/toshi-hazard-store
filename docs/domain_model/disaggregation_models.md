**Tables:**

 - **DisaggAggregationExceedance** - Disaggregation curves of Probablity of Exceedance
 - **DisaggAggregationOccurence** - Disaggregation curves of Probablity of Occurence

The base class **LocationIndexedModel** provides common attributes and indexing for models that support location-based indexing.

The base class **DisaggAggregationBase** defines attribtues common to both types of disaggregation curve.

```mermaid
classDiagram
direction TB

class LocationIndexedModel {

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

}

class DisaggAggregationBase{
    ... fields from LocationIndexedModel
    hazard_model_id = UnicodeAttribute()
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)

    hazard_agg = EnumConstrainedUnicodeAttribute(AggregationEnum)  # eg MEAN
    disagg_agg = EnumConstrainedUnicodeAttribute(AggregationEnum)

    disaggs = CompressedPickleAttribute()  # a very compressible numpy array,
    bins = PickleAttribute()  # a much smaller numpy array

    shaking_level = FloatAttribute()
    probability = EnumAttribute(ProbabilityEnum)  # eg TEN_PCT_IN_50YRS
}

class DisaggAggregationExceedance{
    ... fields from DisaggAggregationBase
}

class DisaggAggregationOccurence{
    ... fields from DisaggAggregationBase
}
LocationIndexedModel <|-- DisaggAggregationBase
DisaggAggregationBase <| -- DisaggAggregationExceedance
DisaggAggregationBase <| -- DisaggAggregationOccurence
```
