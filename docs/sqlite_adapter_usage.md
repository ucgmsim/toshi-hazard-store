
Users may choose to store data locally instead of the default AWS DynamoDB store. Caveats:

 - The complete NSHM_v1.0.4 dataset will likely prove too large for this option.
 - this is single-user only
 - currently we provide no way to migrate data between storage backends (although in principle this should be relatively easy)


## Environment configuration

```
SQLITE_ADAPTER_FOLDER = os.getenv('THS_SQLITE_FOLDER', './LOCALSTORAGE')
USE_SQLITE_ADAPTER = boolean_env('THS_USE_SQLITE_ADAPTER')
```
## CLI for testing

Some examples using the CLI scripts

### Loading Hazard solution data

First, download obtain the exaplme openquake output HDF5 file from http://simple-toshi-ui.s3-website-ap-southeast-2.amazonaws.com/FileDetail/RmlsZToxMDM4NjY2 and extract it to a local filesystem.

Now add this to your local PROD sqlite datastore ....
```
time THS_USE_SQLITE_ADAPTER=1 NZSHM22_HAZARD_STORE_STAGE=PROD\
 poetry run python scripts/store_hazard_v3.py -c -v LOCALSTORAGE/openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMDY3NDMw/calc_1.hdf5\
 T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg== NA NA NA NA

```

**NB:** the script **store_hazard_v3.py** is used in NSHM runzi automation to extract and store the openquake results into the NSHM DynamoDB tables.


### Hazard Solution Metadata (AWS Pynamodb)

using the production datastore ....

```
time THS_USE_SQLITE_ADAPTER=0\
 AWS_PROFILE=chrisbc NZSHM22_HAZARD_STORE_STAGE=PROD NZSHM22_HAZARD_STORE_REGION=ap-southeast-2\
 poetry run python scripts/ths_testing.py get-meta
2024-01-17 17:37:18 toshi_hazard_store.query.hazard_query INFO     sort_key_val: T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==:150
...
2024-01-17 17:37:18 toshi_hazard_store.query.hazard_query INFO     Total 1 hits
locs: GRD_NZ_0_1_NZ34_BA GT: R2VuZXJhbFRhc2s6MTA2NzMyOQ== HId: T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==
get_rlzs Query consumed: 1.0 units
Query returned: 1 items

real    0m1.523s
user    0m1.373s
sys     0m0.957s
```

**NB:** It is also possible to run a local instance of DyanmoDB using docker, and it should work as above if the environment is configured crrectly (TODO: write this up). This is not recommended except for testing.

#### Hazard Solution metadata (Sqlite adapter)

using the locally populated datastore ....

```
> time THS_USE_SQLITE_ADAPTER=1\
 AWS_PROFILE=chrisbc NZSHM22_HAZARD_STORE_STAGE=PROD NZSHM22_HAZARD_STORE_REGION=ap-southeast-2\
 poetry run python scripts/ths_testing.py get-meta
2024-01-17 17:27:29 toshi_hazard_store.query.hazard_query INFO     sort_key_val: T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==:150
2024-01-17 17:27:29 toshi_hazard_store.query.hazard_query INFO     Total 1 hits
locs: NA GT: NA HId: T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==
get_rlzs Query consumed: 0 units
Query returned: 1 items

real    0m1.004s
user    0m1.095s
sys     0m0.954s
```

### Hazard Solution realizations (AWS Pynamodb)

```
time THS_USE_SQLITE_ADAPTER=0\
 AWS_PROFILE=chrisbc NZSHM22_HAZARD_STORE_STAGE=PROD NZSHM22_HAZARD_STORE_REGION=ap-southeast-2\
 poetry run python scripts/ths_testing.py get-rlzs -I5 -L3 -V1 -R2
...
2024-01-17 17:36:01 pynamodb.connection.base DEBUG     Query consumed 1.5 units
2024-01-17 17:36:01 toshi_hazard_store.query.hazard_query INFO     Total 6 hits
m: THS_OpenquakeRealization-PROD<-36.9~174.8, -36.870~174.770:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-36.9~174.8, -36.870~174.770:150:000001:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-41.5~174.0, -41.510~173.950:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-41.5~174.0, -41.510~173.950:150:000001:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-43.5~172.6, -43.530~172.630:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-43.5~172.6, -43.530~172.630:150:000001:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
get_rlzs Query consumed: 9.0 units
Query returned: 6 items

real    0m2.337s
user    0m1.563s
sys     0m0.966s
```

### Hazard Solution realizations (Sqlite adapter)

```
time THS_USE_SQLITE_ADAPTER=1\
 AWS_PROFILE=chrisbc NZSHM22_HAZARD_STORE_STAGE=PROD NZSHM22_HAZARD_STORE_REGION=ap-southeast-2\
  poetry run python scripts/ths_testing.py get-rlzs -I5 -L3 -V1 -R2
2024-01-17 17:32:32 toshi_hazard_store.query.hazard_query INFO     Total 6 hits
m: THS_OpenquakeRealization-PROD<-36.9~174.8, -36.870~174.770:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-36.9~174.8, -36.870~174.770:150:000001:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-41.5~174.0, -41.510~173.950:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-41.5~174.0, -41.510~173.950:150:000001:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-43.5~172.6, -43.530~172.630:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
m: THS_OpenquakeRealization-PROD<-43.5~172.6, -43.530~172.630:150:000001:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==>
get_rlzs Query consumed: 0 units
Query returned: 6 items

real    0m1.019s
user    0m1.051s
sys     0m1.030s
```