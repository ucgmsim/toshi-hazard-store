## FUTURE STATE

These table models are used to store data created by any suitable PSHA engine. 

## Seismic Hazard Model diagram

Different hazard engines, versions and/or configurations may produce compatible calcalution curves.

This model is similar to the current one, except that:

  - the concept of compatible producer configs is supported
  - **HazardRealizationCurve** records are identified solely by internal attributes & relationships. So **toshi_hazard_soluton_id** is removed but can be recorded in **HazardRealizationMeta**.

**TODO:** formalise logic tree branch identification for both source and GMM logic trees so that these are:

 -  a) unique and unambigious, and
 -  b) easily relatable  to **nzshm_model** instances.
 
**Tables:**

- **CompatibleHazardConfig (CHC)** - defines a logical identifier for compatable **HCPCs**. Model managers must ensure that compability holds true.
- **HazardCurveProducerConfig (HCPC)** - stores the unique attributes that define compatible hazard curve producers. 
- **HazardRealizationMeta** - stores metadata common to a set of hazard realization curves.
- **HazardRealizationCurve** - stores the individual hazard realisation curves.
 - **HazardAggregation** - stores the aggregated hazard curves [see ./openquake_models for details](./openquake_models.md)

```mermaid
classDiagram
direction TB

class CompatibleHazardConfig {
    primary_key
}

class HazardCurveProducerConfig {
    primary_key
    fk_compatible_config

    producer_software = UnicodeAttribute()
    producer_version_id = UnicodeAttribute()
    configuration_hash = UnicodeAttribute() 
    configuration_data = UnicodeAttribute() 
}

class HazardRealizationMeta {
    partition_key = UnicodeAttribute(hash_key=True)  # a static value as we actually don't want to partition our data
    sort_key = UnicodeAttribute(range_key=True)

    fk_compatible_config
    fk_producer_config

    created = TimestampAttribute(default=datetime_now)

    ?hazard_solution_id = UnicodeAttribute()
    ?general_task_id = UnicodeAttribute()
    vs30 = NumberAttribute()  # vs30 value

    src_lt = JSONAttribute()  # sources meta as DataFrame JSON
    gsim_lt = JSONAttribute()  # gmpe meta as DataFrame JSON
    rlz_lt = JSONAttribute()  # realization meta as DataFrame JSON
}

class LocationIndexedModel {
    partition_key = UnicodeAttribute(hash_key=True)
    sort_key = UnicodeAttribute(range_key=True)

    nloc_001 = UnicodeAttribute()  # 0.001deg ~100m grid
    etc...
    version = VersionAttribute()
    uniq_id = UnicodeAttribute()

    lat = FloatAttribute()  # latitude decimal degrees
    lon = FloatAttribute()  # longitude decimal degrees
    
    vs30 = EnumConstrainedIntegerAttribute(VS30Enum)
    site_vs30 = FloatAttribute(null=True)

    created = TimestampAttribute(default=datetime_now)    
}

class HazardRealizationCurve {
    ... fields from LocationIndexedModel
    fk_metadata
    fk_compatible_config

    ?source_tags = UnicodeSetAttribute()
    ?source_ids = UnicodeSetAttribute()

    rlz # TODO ID of the realization
    values = ListAttribute(of=IMTValuesAttribute)
}

class HazardAggregation {
    ... fields from LocationIndexedModel

    fk_compatible_config

    hazard_model_id = UnicodeAttribute() e.g. `NSHM_V1.0.4``
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)
    agg = EnumConstrainedUnicodeAttribute(AggregationEnum)
    values = ListAttribute(of=LevelValuePairAttribute)    
}

CompatibleHazardConfig --> "1..*" HazardCurveProducerConfig
HazardRealizationMeta --> "*..1" HazardCurveProducerConfig
HazardRealizationMeta --> "*..1" CompatibleHazardConfig

LocationIndexedModel <|-- HazardRealizationCurve
LocationIndexedModel <|-- HazardAggregation

HazardRealizationCurve --> "*..1" CompatibleHazardConfig
HazardRealizationCurve --> "*..1" HazardRealizationMeta

HazardAggregation --> "*..1" CompatibleHazardConfig
```
