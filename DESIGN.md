# toshi-hazard-store DESIGN

## DynamoDB hash key and range key

the compromise between query flexiblity and read/write perrmance...


# Overview

We already have a batch-efficient table design but  problem with time/cost to upload data (since we're currently using the same nodes that OQ must run on (ie expensive)).

REF Reading
 - https://www.codementor.io/@mohdbelal/writing-millions-of-records-in-dynamodb-14zgyszj41
 - https://towardsdatascience.com/dynamo-exports-may-get-your-data-out-but-this-is-still-the-fastest-way-to-move-data-in-5bcd9748cc00

Based on these we can achieve progressively (and orders of mag)  higher write throughput , following steps below (1 & 2 at least)
 - 1) multi-thread to increase write throughput (to some TBD limit). Needs TUNE/TEST and error handling. DONE
 - 2) separate the hdf5=>dynamodb batch-saving and run this on low cost Fargate nodes. Needs TUNE/TEST and error handling.
 - 3) OR MAYBE ... stream the writes from the actual oq engine task as they are produced (an oq hack) ... (Needs GEM input)


## TESTING

### Baseline - using Toshi ID as hash key

Write usage (average units/s ~ 384

and saving 11500 sites , one Source LTB, 377 IMTS...

```
openquake@e904bbb996ee:/app$ time store_hazard /home/openquake/oqdata/calc_13.hdf5 CALC13 -n -f -v
Begin saving meta
Done saving meta, took 0.56693 secs
Begin saving stats (V2)
Done saving stats, took 715.427528 secs
Begin saving realisations (V2)
Done saving realisations, took 4467.403197 secs

real    86m25.222s
user    49m52.248s
sys     0m43.395s
```

**query example #2 (Near WLG):**
haz_sol_id (Partition key) = `CALC13`
loc_rlz_rk (Sort key) >= `[-41.500~175.250]:00000`

returns 24 records

### Location Range 0.5 degrees - using downsample_loc (Attempt 1 = slower!)

see  toshi_hazard_store.utils.downsample_loc

and commit `b152d65c964d7d43fcfbaabd9e5bd8a0f03f5e98`

Write usage (average units/s ~ 284

and saving 11500 sites , one Source LTB, 377 IMTS...

```
openquake@e904bbb996ee:/app$ time store_hazard /home/openquake/oqdata/calc_13.hdf5 CALC13.DSLOC -n -f -v
Begin saving meta
Done saving meta, took 0.947219 secs
Begin saving stats (V2)
Done saving stats, took 740.48276 secs
Begin saving realisations (V2)
Done saving realisations, took 6042.876689 secs

real    113m6.136s
user    49m11.594s
sys     0m42.257s
```

query example (dynamodDB console):
haz_sol_id (Partition key) =  `-42.5~173.0`
loc_rlz_rk (Sort key) >= `[-42.550~172.750]:00000`

query example #2 (Near WLG):
haz_sol_id (Partition key) = `-41.5~175.0`
loc_rlz_rk (Sort key) >= `[-41.500~175.250]:00000`

returns 2 records

### Back to earlier, run in parallel

commit `c0f91924e48a5eb4c9eef856284e327d0d6cd901`

4 jobs in parallel (copied hdf5)

 - achieved ~1400 units/s
 - 1 of 4 jobs crashed w error

```
...
    File "/opt/openquake/lib/python3.8/site-packages/pynamodb/models.py", line 146, in commit
    data = self.model._get_connection().batch_write_item(
  File "/opt/openquake/lib/python3.8/site-packages/pynamodb/connection/table.py", line 173, in batch_write_item
    return self.connection.batch_write_item(
  File "/opt/openquake/lib/python3.8/site-packages/pynamodb/connection/base.py", line 1141, in batch_write_item
    raise PutError("Failed to batch write items: {}".format(e), e)
pynamodb.exceptions.PutError: Failed to batch write items: An error occurred (InternalServerError) on request (7RPR86NPNVRK22IF1M6VNO8NAFVV4KQNSO5AEMVJF66Q9ASUAAJG) on table (ToshiOpenquakeHazardCurveStatsV2-TEST) when calling the BatchWriteItem operation: Internal server error
```


### Multi-proc 4 workers, using Toshi ID as hash key

added pynamodb_settings to mitigate AWS errors:...

```
base_backoff_ms = 200  # default 25
max_retry_attempts = 8  # default 3
```

run `NZSHM22_HAZARD_STORE_NUM_WORKERS=4 store_hazard /home/openquake/oqdata/calc_131.hdf5 131FAST=4 -n -v`

Write usage (average units/s ~ 980)

saving 11500 sites , one Source LTB, 377 IMTS...

```

worker DynamoBatchWorker-8 saving batch of len: 34
worker DynamoBatchWorker-5 saving batch of len: 50
worker DynamoBatchWorker-6 saving batch of len: 16
worker DynamoBatchWorker-6 saving batch of len: 16
DynamoBatchWorker-7: Exiting
worker DynamoBatchWorker-7 saving batch of len: 26
DynamoBatchWorker-6: Exiting
DynamoBatchWorker-8: Exiting
DynamoBatchWorker-5: Exiting
Done saving realisations, took 1586.058368 secs
```
