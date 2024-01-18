**Tables:**

 - **GriddedHazard** - Grid points defined in location_grid_id has a values in grid_poes.
 - **HazardAggregation** - stores aggregate hazard curves [see ./openquake_models for details](./openquake_models.md)

```mermaid
classDiagram
direction LR

class GriddedHazard{
    partition_key = UnicodeAttribute(hash_key=True)
    sort_key = UnicodeAttribute(range_key=True)
    version = VersionAttribute()
    created = TimestampAttribute(default=datetime_now)
    hazard_model_id = UnicodeAttribute()
    location_grid_id = UnicodeAttribute()
    vs30 = EnumConstrainedIntegerAttribute(VS30Enum)
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)
    agg = EnumConstrainedUnicodeAttribute(AggregationEnum)
    poe = FloatAttribute()
    grid_poes = CompressedListAttribute()
}

GriddedHazard --> "1..*" HazardAggregation
```
