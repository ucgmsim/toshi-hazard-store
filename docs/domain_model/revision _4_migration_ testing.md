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

| Hazard calculation ID                | HDF5 size | Revision | Service  |Object count | Workers | Time   | Units/Sec avg | Unit Cost |
|--------------------------------------|-----------|----------|----------|-------------|---------|--------|---------------|-----------|
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb    | V3       | sqlite3  | 83811       | 1       | 2m50   | -             | -         |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb    | V4       | dynamodb | 2262897     | 1       | 14m11  | -             | -         |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3 | 2.0 Gb    | V4       | dynamodb | 2262897     | 1       | 13m46  | -             | -         |

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

