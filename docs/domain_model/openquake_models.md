## CURRENT STATE

These table models are used to store data created by GEMs **openquake** PSHA engine. Data is extracted from the HDF5 files created by openquake and stored with relevant metadata in the following tables.

## Seismic Hazard Model diagram

**Tables:**

 - **ToshiOpenquakeMeta** - stores metadata from the job configuration and the openquake results.

```mermaid
classDiagram
direction LR

class ToshiOpenquakeMeta {
    partition_key = UnicodeAttribute(hash_key=True)  # a static value as we actually don't want to partition our data
    hazsol_vs30_rk = UnicodeAttribute(range_key=True)

    created = TimestampAttribute(default=datetime_now)

    hazard_solution_id = UnicodeAttribute()
    general_task_id = UnicodeAttribute()
    vs30 = NumberAttribute()  # vs30 value

    imts = UnicodeSetAttribute()  # list of IMTs
    locations_id = UnicodeAttribute()  # Location codes identifier (ENUM?)
    source_ids = UnicodeSetAttribute()
    source_tags = UnicodeSetAttribute()
    inv_time = NumberAttribute()  # Invesigation time in years

    src_lt = JSONAttribute()  # sources meta as DataFrame JSON
    gsim_lt = JSONAttribute()  # gmpe meta as DataFrame JSON
    rlz_lt = JSONAttribute()  # realization meta as DataFrame JSON
}
```

**Tables:**

 - **OpenquakeRealization** -  stores the individual hazard realisation curves.
 - **HazardAggregation** - stores aggregate hazard curves from **OpenquakeRealization** curves.

The base class **LocationIndexedModel** provides common attributes and indexing for models that support location-based indexing.


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

class OpenquakeRealization {
    ... fields from LocationIndexedModel
    hazard_solution_id = UnicodeAttribute()
    source_tags = UnicodeSetAttribute()
    source_ids = UnicodeSetAttribute()

    rlz = IntegerAttribute()  # index of the openquake realization
    values = ListAttribute(of=IMTValuesAttribute)
}

class HazardAggregation {
    ... fields from LocationIndexedModel
    hazard_model_id = UnicodeAttribute() e.g. `NSHM_V1.0.4``
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)
    agg = EnumConstrainedUnicodeAttribute(AggregationEnum)
    values = ListAttribute(of=LevelValuePairAttribute)    
}


ToshiOpenquakeMeta --> "0..*"  OpenquakeRealization
HazardAggregation --> "1..*" OpenquakeRealization
LocationIndexedModel <|-- OpenquakeRealization
LocationIndexedModel <|-- HazardAggregation

```