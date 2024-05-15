# PROCESSING May 14, 2024


## build the dataset for GT R2VuZXJhbFRhc2s6MTMyODQxNA==

 - this includes the AWS PROD tables: THS_R4_CompatibleHazardCalculation-PROD, THS_R4_HazardCurveProducerConfig-PROD
 - but no rlz curves are stored in AWS
 - instead they're stored to dataset: WORKING/ARROW/THS_R4_IMPORT

```
AWS_PROFILE=chrisbc poetry run ths_r4_import producers R2VuZXJhbFRhc2s6MTMyODQxNA== A -O WORKING/ARROW/THS_R4_IMPORT -v -T ARROW -CCF A_NZSHM22-0 --with_rlzs -W WORKING

 - built 3,264 items, totalling 33.2B
 - approx 15m ( had to download one archive)
```


## High level sanity

- counts rlzs looks good
- took 25 m
```
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-store$ AWS_PROFILE=chrisbc time poetry run python scripts/migration/ths_r4_sanity.py count-rlz -S ARROW -D THS_R4_IMPORT -R ALL
INFO:pynamodb.settings:Override settings for pynamo available /etc/pynamodb/global_default_settings.py
querying arrow/parquet dataset THS_R4_IMPORT
calculation_id, uniq_rlzs, uniq_locs, uniq_imts, uniq_gmms, uniq_srcs, uniq_vs30, consistent
============================================================================================
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0MA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0MQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Mg==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Mw==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0NA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0NQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Ng==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Nw==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0OA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0OQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1MA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1MQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Mg==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Mw==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1NA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1NQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Ng==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Nw==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1OA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1OQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU2MA==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU2MQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxMw==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNA==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNQ==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNg==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNw==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxOA==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxOQ==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMA==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMQ==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMg==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMw==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNA==, 1293084, 3991, 27, 12, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNg==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNw==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyOA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyOQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMg==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMw==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNg==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNw==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzOA==, 2262897, 3991, 27, 21, 1, 1, True
T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzOQ==, 2262897, 3991, 27, 21, 1, 1, True

Grand total: 98274384
16092.82user 1472.55system 25:43.27elapsed 1138%CPU (0avgtext+0avgdata 19966420maxresident)k
215064240inputs+8outputs (1368major+261581604minor)pagefaults 0swaps
```

## SPOT CHECKS

```
$ AWS_PROFILE=chrisbc poetry run python scripts/migration/ths_r4_sanity.py random-rlz-new -D WORKING/ARROW/THS_R4_IMPORT 10
INFO:pynamodb.settings:Override settings for pynamo available /etc/pynamodb/global_default_settings.py
[{'created': 1679389905, 'hazard_solution_id': 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==', 'index1_rk': '-34.7~173.0:275:000018:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==', 'lat': -34.7, 'lon': 173, 'nloc_0': '-35.0~173.0', 'nloc_001': '-34.700~173.000', 'nl
oc_01': '-34.70~173.00', 'nloc_1': '-34.7~173.0', 'partition_key': '-34.7~173.0', 'rlz': 18, 'sort_key': '-34.700~173.000:275:000018:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==', 'source_ids': ['"SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE0OTQ=', 'RmlsZToxMzA3MzE="'], 's
ource_tags': ['N4.6', '"geodetic', 'TI', 'b1.089 C4.2 s1.41"'], 'uniq_id': 'ebdce59a-dbea-4693-9492-6e04c5726e4d', 'values': [{'imt': 'SA(2.0)', 'lvls': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1,
0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 9, 10], 'vals': [0.11990661174058914, 0.11979455500841141, 0.11848633736371994, 0.11552842706441879, 0.11143646389245987, 0.10676522552967072, 0.08372
888714075089, 0.05430911108851433, 0.038751404732465744, 0.029467571526765823, 0.023378757759928703, 0.010211423970758915, 0.003731991397216916, 0.0019087463151663542, 0.0011496058432385325, 0.0007645023288205266, 0.00020399079949129373, 9.229181887349114e-05, 5.2
03131877351552e-05, 3.291014218120836e-05, 2.2294307200354524e-05, 1.5800043911440298e-05, 1.1562286090338603e-05, 8.668140253575984e-06, 6.62346201352193e-06, 4.042109594593057e-06, 2.5812723833951168e-06, 1.7068604165615398e-06, 1.160888245976821e-06, 8.08214394
9650344e-07, 5.739782409364125e-07, 4.1469243683422974e-07, 3.04156145602974e-07, 2.2608764993492514e-07, 1.700683185390517e-07, 8.718910748939379e-08, 4.701103506477011e-08, 2.637871965305294e-08, 1.528405668693722e-08, 5.4942366212173965e-09, 2.0868156092035406e
-09, 8.132976514474421e-10, 3.2267499783245057e-10, 1.2921619330086287e-10]}], 'vs30': 275, 'slt_sources': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE0OTQ=|RmlsZToxMzA3MzE=', 'sources_digest': 'f63c42d662b6', 'gsim_uncertainty': GMCMBranch(branch_id='', weight=0.0, gsim_
name='Bradley2013', gsim_args={'sigma_mu_epsilon': '1.28155'}, tectonic_region_type=''), 'gmms_digest': '1da506674d60'}, {'created': 1679390014, 'hazard_solution_id': 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==', 'index1_rk': '-35.9~174.6:275:000018:T3BlbnF1YWtl
SGF6YXJkU29sdXRpb246MTMyODUzNQ==', 'lat': -35.9, 'lon': 174.6, 'nloc_0': '-36.0~175.0', 'nloc_001': '-35.900~174.600', 'nloc_01': '-35.90~174.60', 'nloc_1': '-35.9~174.6', 'partition_key': '-35.9~174.6', 'rlz': 18, 'sort_key': '-35.900~174.600:275:000018:T3BlbnF1Y
WtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==', 'source_ids': ['"SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE0OTQ=', 'RmlsZToxMzA3MzE="'], 'source_tags': ['N4.6', '"geodetic', 'TI', 'b1.089 C4.2 s1.41"'], 'uniq_id': '71c1ea9d-ceb5-4580-b114-e3b7b17fc499', 'values': [{'imt': 'SA(2.0)
', 'lvls': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8, 3, 3.5, 4, 4.5, 5, 6, 7, 8, 9, 10], 'vals': [0.179241657
25708008, 0.17904438078403473, 0.1768815517425537, 0.17218109965324402, 0.1658313274383545, 0.1586896777153015, 0.12412205338478088, 0.08041348308324814, 0.05732831731438637, 0.043558891862630844, 0.034526802599430084, 0.014915387146174908, 0.005188937298953533, 0
.002507168101146817, 0.0014362862566486, 0.0009171321289613843, 0.00022070664272177964, 9.670317376730964e-05, 5.385170879890211e-05, 3.3852509659482166e-05, 2.2848900698591024e-05, 1.61524694703985e-05, 1.179736227641115e-05, 8.830375008983538e-06, 6.738417141605
169e-06, 4.103558239876293e-06, 2.6163356778852176e-06, 1.7278762243222445e-06, 1.1739224419216043e-06, 8.165166605067498e-07, 5.793877448923013e-07, 4.1828798202914186e-07, 3.0658341643174936e-07, 2.2775374475259014e-07, 1.7122340523201274e-07, 8.76810517524973e-
08, 4.724623892116142e-08, 2.6498927496731994e-08, 1.53471972907937e-08, 5.515145229395557e-09, 2.0973296432913457e-09, 8.223531855477972e-10, 3.3296709833763316e-10, 1.403526184162729e-10]}], 'vs30': 275, 'slt_sources': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE0OTQ=|R
mlsZToxMzA3MzE=', 'sources_digest': 'f63c42d662b6', 'gsim_uncertainty': GMCMBranch(branch_id='', weight=0.0, gsim_name='Bradley2013', gsim_args={'sigma_mu_epsilon': '1.28155'}, tectonic_region_type=''), 'gmms_digest': '1da506674d60'}]
model match {'tid': 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==', 'imt': 'SA(2.0)', 'rlz': 18, 'locs': [CodedLocation(lat=-35.9, lon=174.6, resolution=0.001), CodedLocation(lat=-34.7, lon=173.0, resolution=0.001), CodedLocation(lat=-43.8, lon=171.4, resolution=0
.001), CodedLocation(lat=-45.1, lon=171.1, resolution=0.001), CodedLocation(lat=-46.3, lon=168.4, resolution=0.001), CodedLocation(lat=-40.0, lon=176.3, resolution=0.001), CodedLocation(lat=-44.6, lon=167.6, resolution=0.001), CodedLocation(lat=-46.1, lon=166.6, r
esolution=0.001), CodedLocation(lat=-39.8, lon=175.5, resolution=0.001), CodedLocation(lat=-39.3, lon=175.6, resolution=0.001)]}
model match {'tid': 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==', 'imt': 'SA(2.0)', 'rlz': 18, 'locs': [CodedLocation(lat=-35.9, lon=174.6, resolution=0.001), CodedLocation(lat=-34.7, lon=173.0, resolution=0.001), CodedLocation(lat=-43.8, lon=171.4, resolution=0
.001), CodedLocation(lat=-45.1, lon=171.1, resolution=0.001), CodedLocation(lat=-46.3, lon=168.4, resolution=0.001), CodedLocation(lat=-40.0, lon=176.3, resolution=0.001), CodedLocation(lat=-44.6, lon=167.6, resolution=0.001), CodedLocation(lat=-46.1, lon=166.6, r
esolution=0.001), CodedLocation(lat=-39.8, lon=175.5, resolution=0.001), CodedLocation(lat=-39.3, lon=175.6, resolution=0.001)]}
...

etc
```

## DEFRAG

```
time AWS_PROFILE=chrisbc poetry run python scripts/ths_arrow_compaction.py WORKING/ARROW/THS_R4_IMPORT/ WORKING/ARROW/THS_R4_DEFRAG
partition (nloc_0 == "-41.0~175.0")
compacted WORKING/ARROW/THS_R4_DEFRAG
...
partition (nloc_0 == "-44.0~171.0")
compacted WORKING/ARROW/THS_R4_DEFRAG
partition (nloc_0 == "-37.0~175.0")
compacted WORKING/ARROW/THS_R4_DEFRAG
compacted 64 partitions for WORKING/ARROW

real    9m6.216s
user    67m18.559s
sys     8m42.890s
```


## High level sanity

**CRASHES - machine** - goes into swap , consuming all the memory ..

## DEFRAG attempt 2 ....

 - nloc-000 only

 a few minutes ->    32 GB


## DEFRAG 2 LEVELS

 - 90 GB

## Float 32

```
time AWS_PROFILE=chrisbc poetry run ths_r4_import producers R2VuZXJhbFRhc2s6MTMyODQxNA== A -O WORKING/ARROW/THS_R4_F32 -v -T ARROW -CCF A_NZSHM22-0 --with_rlzs -W WORKING
...
real    8m40.173s
user    14m2.189s
sys     2m30.889s
```

### DEFRAG 3 levels

 - 90 GB

```
time AWS_PROFILE=chrisbc poetry run python scripts/ths_arrow_compaction.py WORKING/ARROW/THS_R4_F32 WORKING/ARROW/THS_R4_F32_DEFRAG

partition (nloc_0 == "-37.0~175.0")
compacted WORKING/ARROW/THS_R4_F32_DEFRAG
compacted 64 partitions for WORKING/ARROW

real    8m2.617s
```