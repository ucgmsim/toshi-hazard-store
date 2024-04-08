# Table migration testing

This describes performance of the v3 and v4 tables in sqlite and dynamodb


## Test outline

We used a Typical NSHM General Task **R2VuZXJhbFRhc2s6MTMyODQxNA==** with large number of sites and all four tectonic regions as used in **NSHM_V1.0.4**.

We test read and write performance in terms of time and AWS unit costs. For AWS different worker counts are tested.


## PynamoDB tests 

**April 1st 2024**

These are conducted on TryHarder (16 core workstation) from Masterton NZ, connected to the **us-east-1** dynamodb service.

Tested Tables:
V3: THS_OpenquakeRealization-TEST_CBC
V4: THS_R4_HazardRealizationCurve-TEST_CBC

| Hazard task ID                       | HDF5 size | Revision | Service  |Object count | Workers | Time   | Units/Sec avg | Unit Cost |
|--------------------------------------|-----------|----------|----------|-------------|---------|--------|---------------|-----------|
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb    | V3       | sqlite3  | 83811       | 1       | 2m50   | -             | -         |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb    | V4       | sqlite3  | 2262897     | 1       | 14m11  | -             | -         |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3 | 2.0 Gb    | V4       | sqlite3  | 2262897     | 1       | 13m46  | -             | -         |

| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb    | V3       | dynamodb | 83811       | 4       | 29m6   | 1800          | ?         |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb    | V4       | dynamodb | 2262897     | 4       | 248m54 | 150           | ?         |

| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb    | V4       | dynamodb | 2262897     | 24      | 26m29  | 1900          | ?         |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3 | 2.0 Gb    | V3       | dynamodb | 83811       | 8       | 15m4   | 3500          | ?         |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDMz | 2.0.Gb    | V3       | dynamodb | 83811       | 12      | 14m26  | 4500          | ?         |

| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI5 | 855Mb     | V4       | sqlite3  | 1293084     | 1       | 6m59   | -             | -         |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI5 | 855Mb     | V3       | dynamodb | 47892       | 1       | 1m41   | -             | -         |


# after adding openquake hdf5 manipulation

| 1st 6 tasks    | V3 | LOCAL | 11.1 Gb | 11m2s |
| 1st 6 tasks    | V4 | LOCAL | 10.9 Gb | 51m52 |


## notes on Pynamodb costs

I've not been able to get the custom loghandler working properly for this setup. It's weird as the code is called but somehow the handlers state is not being updated.


see https://docs.python.org/3/howto/logging-cookbook.html#a-more-elaborate-multiprocessing-example

but leaving this for another day ....


```
2024-04-03 13:43:42 DEBUG    pynamodb.connection.base  BatchWriteItem consumed [{'TableName': 'THS_OpenquakeRealization-TEST_CBC', 'CapacityUnits': 514.0}] units
2024-04-03 13:43:42 INFO     toshi_hazard_store.multi_batch Saved batch of 17 <class 'toshi_hazard_store.model.openquake_models.OpenquakeRealization'> models
2024-04-03 13:43:42 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-9: Exiting
2024-04-03 13:43:42 INFO     toshi_hazard_store.multi_batch save_parallel completed 47892 tasks.
pyanmodb operation cost: 5.0 units
```

Cost observations:

  - Rev 4 are a 1 unit/object, as total objects size is just under 1k
  - Ver 3 vary, as a) batching uses different sizes and b) the objects are much larger (~17 times ?).


## April 4th 2024

 - **ths_r4_migrate** migrated 10.5 GB V3 local into V4 local in around 1 hour. Realisations that is.
 - **ths_r4_migrate** migration from PROD ap-southeast-2 tables to LOCAL is very slow - about 120 times slower due to read latency.

 ```
 INFO:scripts.ths_r4_migrate:Processing calculation T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNQ== in gt R2VuZXJhbFRhc2s6MTMyODQxNA==
 INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
 INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
 INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-1 saved 10000 HazardRealizationCurve objects in 847.937410 seconds with batch size 100
 ```

# new process testing

## 1) import from HDF5 to LOCAL V3 ...

```
$ time poetry run ths_r4_import -W WORKING producers R2VuZXJhbFRhc2s6MTMyODQxNA== A -CCF A_A --with_rlzs -P3
...
NZNSHM2022_KuehnEtAl2020SInter_GLO
2024-04-04 15:40:56 INFO     toshi_hazard_store.multi_batch Creating 1 workers
2024-04-04 15:40:56 INFO     toshi_hazard_store.multi_batch worker DynamoBatchWorker-1 running with batch size: 1000
2024-04-04 15:41:29 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-1 saved 10000 OpenquakeRealization objects in 33.539089 seconds with batch size 1000
2024-04-04 15:41:42 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-1 saved 10000 OpenquakeRealization objects in 12.737609 seconds with batch size 1000
2024-04-04 15:41:52 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-1 saved 10000 OpenquakeRealization objects in 10.113900 seconds with batch size 1000
2024-04-04 15:42:02 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-1 saved 10000 OpenquakeRealization objects in 9.563776 seconds with batch size 1000
2024-04-04 15:42:08 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-1: Exiting
2024-04-04 15:42:09 INFO     toshi_hazard_store.multi_batch Saved final 892 <class 'toshi_hazard_store.model.openquake_models.OpenquakeRealization'> models
2024-04-04 15:42:09 INFO     toshi_hazard_store.multi_batch save_parallel completed 47892 tasks.
pyanmodb operation cost: 0 units

real    1m28.584s

```
## 2) now migrate (using 8 workers)...
first remember to set: `THS_USE_SQLITE_ADAPTER=False` as this determines the write target

```
time poetry run ths_r4_migrate -W WORKING R2VuZXJhbFRhc2s6MTMyODQxNA== A A_A -S LOCAL -T AWS

...

INFO:scripts.ths_r4_migrate:Processing calculation T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNA== in gt R2VuZXJhbFRhc2s6MTMyODQxNA==
INFO:toshi_hazard_store.oq_import.migrate_v3_to_v4:Configure adapter: <class 'toshi_hazard_store.db_adapter.sqlite.sqlite_adapter.SqliteAdapter'>
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-5 saved 10000 HazardRealizationCurve objects in 84.680788 seconds with batch size 25
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-20 saved 10000 HazardRealizationCurve objects in 84.477392 seconds with batch size 25
...
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-1: Exiting
INFO:toshi_hazard_store.multi_batch:Saved final 20 <class 'toshi_hazard_store.model.revision_4.hazard_models.HazardRealizationCurve'> models
INFO:toshi_hazard_store.multi_batch:save_parallel completed 1249020 tasks.

real    30m57.451s

```
*Getting write units/sec around 700*


## 1B) ths_r4_import (1st six hazard calcs)

```
time poetry run ths_r4_import -W WORKING producers R2VuZXJhbFRhc2s6MTMyODQxNA== A -CCF A_A --with_rlzs -P3
...
024-04-04 16:56:13 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-6 saved 10000 OpenquakeRealization objects in 14.054833 seconds with batch size 1000
2024-04-04 16:56:27 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-6 saved 10000 OpenquakeRealization objects in 13.857113 seconds with batch size 1000
2024-04-04 16:56:37 INFO     toshi_hazard_store.multi_batch DynamoBatchWorker-6: Exiting
2024-04-04 16:56:38 INFO     toshi_hazard_store.multi_batch Saved final 892 <class 'toshi_hazard_store.model.openquake_models.OpenquakeRealization'> models
2024-04-04 16:56:38 INFO     toshi_hazard_store.multi_batch save_parallel completed 47892 tasks.
pyanmodb operation cost: 0 units

real    10m49.324s
user    10m36.213s
sys     0m26.076s
```

## 1B) migrate to AWS (1st of six hazard calcs, using 36 workers)
```
$ time poetry run ths_r4_migrate -W WORKING R2VuZXJhbFRhc2s6MTMyODQxNA== A A_A -S LOCAL -T AWS
Warning: 'ths_r4_migrate' is an entry point defined in pyproject.toml, but it's not installed as a script. You may get improper `sys.argv[0]`.

...
INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
INFO:toshi_hazard_store.multi_batch:Creating 36 workers
...
INFO:pynamodb.models:Resending 14 unprocessed keys for batch operation (retry 1)
INFO:pynamodb.models:Resending 23 unprocessed keys for batch operation (retry 1)
...
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-13 saved 10000 HazardRealizationCurve objects in 124.565442 seconds with batch size 25
...
INFO:pynamodb.models:Resending 9 unprocessed keys for batch operation (retry 1)
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-3 saved 10000 HazardRealizationCurve objects in 131.398797 seconds with batch size 25
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-17 saved 10000 HazardRealizationCurve objects in 135.980906 seconds with batch size 25
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-30 saved 10000 HazardRealizationCurve objects in 132.079633 seconds with batch size 25
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-7 saved 10000 HazardRealizationCurve objects in 136.626109 seconds with batch size 25
...
INFO:toshi_hazard_store.multi_batch:save_parallel completed 2185785 tasks.

real    12m55.306s
user    64m31.855s
sys     0m54.037s
```

**2,185,785** rlzs at around 3k units/sec

## 1C) migrate to AWS (2nd of six hazard calcs, using 24 workers)


```

$ time poetry run ths_r4_migrate -W WORKING R2VuZXJhbFRhc2s6MTMyODQxNA== A A_A -S LOCAL -T AWS
...
INFO:toshi_hazard_store.multi_batch:DynamoBatchWorker-20: Exiting
INFO:toshi_hazard_store.multi_batch:save_parallel completed 9367650 tasks.

real    74m48.851s
user    296m32.444s
sys     3m36.473s
```

**9,367,650** rlzs at around 2.1k units/sec


## 2A (for remainder of R2VuZXJhbFRhc2s6MTMyODQxNA==)

much slower this time as hdf5 download and processing is needed.

```
time poetry run ths_r4_import -W WORKING producers R2VuZXJhbFRhc2s6MTMyODQxNA== A -CCF A_A --with_rlzs -P3
...
2024-04-04 20:48:35 INFO     toshi_hazard_store.multi_batch Saved final 811 <class 'toshi_hazard_store.model.openquake_models.OpenquakeRealization'> models
2024-04-04 20:48:35 INFO     toshi_hazard_store.multi_batch save_parallel completed 83811 tasks.
pyanmodb operation cost: 0 units

real    206m34.483s
user    109m4.812s
sys     9m16.969s
```

## 3B TODO



# Sanity checks

Now after importing all 49 from GT **R2VuZXJhbFRhc2s6MTMyODQxNA==** we have a sqlite db (**THS_OpenquakeRealization_TEST_CBC.db** in LOCALSTORAGE folder of **112 Gb**.

Dimensions:

 - **locations**: all NZ 01 grid (note NZ34, SRWG, Transpower omitted)
 - **vs30**: 1 (275)
 - **imt**: all
 - **
 - **V3 only**:
    **RLZ** by id : all (11 - 21, depending on tectonic region)
    **Task IDs**: all 49 from GT R2VuZXJhbFRhc2s6MTMyODQxNA==
    **Row/object count:** 3639792

Goals: confirm that

 a. the new imported data is equivalent to what we have in DynamoDB table (ap-southeast-2/THS_OpenquakeRealization_PROD), and
 b. all the data we intended is actually available


Checks:

  - [ ] count of imported objects (LOCAL: **3639792**) matches the equivalent query against Dynamodb. PROD : **3411792** NO nw table is bigger by 200K!! (See below....)
  - [X] spot-check 1000 random realisation curves. Random location, IMT, RLZ ID,




## Investigating rlz counts in the two DBs...

OK, LOCAL has an extra 250 locations saved from every calc...

This was discovered by local SQL ...

```
$ sqlite3 LOCALSTORAGE/THS_OpenquakeRealization_TEST_CBC.db "select nloc_001, max(sort_key) from THS_OpenquakeRealization_TEST_CBC WHERE hazard_solution_id = 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzOQ==' GROUP BY nloc_001;" -separator "," > scripts/migration/ths_r4_sanity.local_by_nloc_001.csv
```

and with a little python set analysis....

```

>>> import pathlib
>>> sane_csv = pathlib.Path( "scripts/migration/ths_r4_sanity.local_by_nloc_001.csv" )
>>> locs = [row[0] for row in csv.reader(open(sane_csv))]
>>> len(locs)
3991
>>> locs[:10]
['-34.300~172.900', '-34.300~173.000', '-34.300~173.100', '-34.400~172.600', '-34.400~172.700', '-34.400~172.800', '-34.400~172.900', '-34.400~173.000', '-34.400~173.100', '-34.500~172.600']
>>>
>>> from nzshm_common.grids import load_grid
>>> from nzshm_common.location.code_location import CodedLocation
>>> nz1_grid = load_grid('NZ_0_1_NB_1_1')
>>> grid_locs = [CodedLocation(o[0], o[1], 0.001).code for o in nz1_grid]
>>> gs = set(grid_locs)
>>> ls = set(locs)
>>> ls.differnce(gs)
>>> ls.difference(gs)
{'-45.873~170.368', '-39.929~175.033', '-37.780~175.280', '-36.870~174.770', '-37.242~175.026', '-43.144~170.570', '-41.295~174.896', '-45.054~169.182', '-38.664~178.022', '-43.747~172.009', '-40.337~175.866', '-42.830~171.562', '-45.410~167.720', '-38.016~177.275', '-40.908~175.001', '-43.526~172.365', '-36.393~174.656', '-41.747~171.617', '-41.510~173.950', '-41.111~174.852', '-41.082~175.454', '-43.322~172.666', '-37.000~175.850', '-41.802~172.335', '-42.112~171.859', '-44.268~170.096', '-41.281~174.018', '-44.094~171.243', '-44.379~171.230', '-40.464~175.231', '-40.350~175.620', '-45.870~170.500', '-35.283~174.087', '-44.943~168.832', '-43.633~171.643', '-41.377~173.108', '-41.300~174.780', '-43.724~170.091', '-40.221~175.564', '-44.989~168.673', '-38.230~175.870', '-43.306~172.593', '-41.750~171.580', '-42.719~170.971', '-38.224~175.868', '-44.731~171.047', '-40.455~175.837', '-42.490~171.185', '-38.997~174.234', '-37.686~176.168', '-45.248~169.382', '-39.451~173.859', '-37.815~175.773', '-37.211~175.866', '-45.600~170.678', '-43.376~170.188', '-39.930~175.050', '-40.960~175.660', '-43.311~172.697', '-38.650~178.000', '-41.367~173.143', '-39.480~176.920', '-39.119~173.953', '-39.470~175.678', '-36.758~174.584', '-43.579~172.508', '-46.122~169.968', '-43.350~170.170', '-39.999~176.546', '-43.880~169.060', '-35.629~174.507', '-39.587~176.913', '-38.010~175.328', '-46.365~168.015', '-43.730~170.100', '-37.897~178.319', '-43.531~172.637', '-35.830~174.460', '-35.720~174.320', '-45.414~167.723', '-43.635~172.725', '-38.137~176.260', '-37.643~176.188', '-40.954~175.651', '-42.523~172.824', '-37.138~174.708', '-40.550~175.413', '-39.157~174.201', '-39.685~176.885', '-39.413~175.407', '-43.604~172.389', '-42.812~173.274', '-40.206~176.102', '-44.097~170.825', '-42.413~173.677', '-38.695~176.079', '-41.261~174.945', '-38.053~175.785', '-40.860~172.818', '-40.291~175.759', '-41.289~174.777', '-43.295~172.187', '-43.641~172.487', '-46.103~168.939', '-45.481~170.710', '-37.950~176.971', '-44.614~169.267', '-43.603~172.709', '-40.972~174.969', '-43.530~172.630', '-43.807~172.969', '-43.384~172.657', '-39.938~176.592', '-44.991~168.802', '-46.187~168.873', '-44.400~171.260', '-36.818~175.691', '-39.632~176.832', '-41.282~174.776', '-46.145~168.324', '-37.653~175.528', '-44.257~171.136', '-36.293~174.522', '-41.508~173.826', '-37.671~175.151', '-41.212~174.903', '-35.280~174.054', '-45.085~170.971', '-44.673~167.925', '-46.430~168.360', '-41.220~175.459', '-40.071~175.376', '-44.690~169.148', '-38.335~175.170', '-35.309~174.102', '-45.192~169.324', '-36.770~174.543', '-37.788~176.311', '-35.594~174.287', '-36.756~175.496', '-41.831~174.125', '-36.992~174.882', '-40.630~175.290', '-43.463~170.012', '-40.750~175.117', '-45.023~168.719', '-37.041~175.847', '-39.338~174.285', '-42.450~171.210', '-39.431~174.299', '-37.543~175.705', '-41.411~173.044', '-41.800~172.868', '-37.266~174.945', '-35.879~174.457', '-39.678~175.797', '-37.552~175.925', '-39.490~176.918', '-36.658~174.436', '-37.130~175.530', '-37.375~175.674', '-41.270~173.280', '-39.039~177.419', '-36.888~175.038', '-37.428~175.956', '-38.454~176.707', '-41.667~174.071', '-37.643~176.034', '-39.429~175.276', '-42.400~173.680', '-42.944~171.564', '-37.974~176.829', '-35.382~174.070', '-38.883~175.277', '-37.788~175.282', '-41.252~173.095', '-36.790~175.037', '-46.238~169.740', '-41.116~175.327', '-42.393~171.250', '-36.852~174.763', '-36.894~175.002', '-40.362~175.618', '-37.977~177.087', '-38.367~175.774', '-43.892~171.771', '-35.220~173.970', '-36.777~174.479', '-40.180~175.382', '-41.121~173.004', '-43.496~172.094', '-36.611~174.733', '-43.155~172.731', '-37.890~175.462', '-36.675~174.454', '-38.183~175.217', '-35.408~173.798', '-37.251~174.736', '-40.754~175.142', '-37.807~174.867', '-37.188~174.829', '-38.089~176.691', '-39.593~174.275', '-40.855~175.061', '-45.938~170.358', '-35.983~174.444', '-37.408~175.141', '-39.058~174.081', '-46.607~168.332', '-37.387~175.843', '-37.690~176.170', '-35.939~173.865', '-38.993~175.806', '-37.201~174.910', '-39.754~174.470', '-35.386~174.022', '-36.420~174.725', '-37.279~175.492', '-46.412~168.347', '-43.760~172.297', '-38.089~176.213', '-37.980~177.000', '-40.630~175.280', '-35.230~173.958', '-39.070~174.080', '-41.124~175.070', '-39.590~174.280', '-37.548~175.160', '-44.242~171.288', '-39.000~175.930', '-42.780~171.540', '-42.334~172.182', '-41.520~173.948', '-38.140~176.250', '-38.680~176.080', '-45.020~168.690', '-46.901~168.136', '-41.028~175.520', '-45.874~170.504', '-40.477~175.305', '-35.109~173.262', '-37.994~175.205', '-37.155~175.555', '-41.341~173.182', '-35.719~174.318', '-38.037~175.337', '-42.540~172.780', '-36.094~174.584', '-41.271~173.284', '-36.825~174.429'}

>>> len(ls.difference(gs))
250

>>> # show that the rumber of RLZ * extra locations (912 * 250) == 228000. This exacly equals the Total difference. See working files in scripts/migraion folder.
>>> (250 * 912) == (3639792 - 3411792)
True

```
Are these the SWRG sites ??? YES looks like it:
```
    {
        "id": "srg_202",
        "name": "Mosgiel",
        "latitude": -45.873290138,
        "longitude": 170.367548721
    },
```


### Spot checking one location = OK

these use the new ths_r4_sanity.py script

```
time poetry run sanity count-rlz -S AWS
```

which counts realisations by hazard_solution_id for `-42.450~171.210`

#### LOCAL (real    0m0.969s)

```
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMg==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyOQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMQ==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxOQ==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNw==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNg==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyOA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxOA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxMw==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMw==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNQ==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0OA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Nw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Mg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1MA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Nw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Ng==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0OQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU2MQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Mw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Mg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1OQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1NQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0MQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzOQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Ng==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0NA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0NQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU2MA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1NA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0MA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1MQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzOA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Mw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1OA==, 21

Total 912

real    0m0.969s
```

### DynamoDB (real    47m42.010s)

```
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMg==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyOQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMQ==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxOQ==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNw==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNg==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyOA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxOA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxMw==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMw==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNQ==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyNA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0OA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Nw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Mg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1MA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Nw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Ng==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0OQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU2MQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Mw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Mg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1OQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1NQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0MQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzOQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzNg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMg==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1Ng==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0NA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0NQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU2MA==, 12
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1NA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0MA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1MQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzMQ==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUzOA==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU0Mw==, 21
-42.450~171.210, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU1OA==, 21
```

real    47m42.010s


## Spot checking random curves...

```
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-store$ poetry run sanity random-rlz 75
...
compared 4943 realisations with 0 material differences
```



# pyarrow experiments

write to arrow file first 12 (25%) = 9.3GB in 10000 row df batched

```

time poetry run ths_r4_migrate -W WORKING/ R2VuZXJhbFRhc2s6MTMyODQxNA== A A_A -S LOCAL -T ARROW
...
INFO:scripts.ths_r4_migrate:built dataframe 1873
INFO:scripts.ths_r4_migrate:Produced 1249020 source objects from T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUyMA== in R2VuZXJhbFRhc2s6MTMyODQxNA==
INFO:scripts.ths_r4_migrate:built dataframe 1874

real    122m58.576s
```