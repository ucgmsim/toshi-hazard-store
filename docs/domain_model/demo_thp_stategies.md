demo_thp_stategies.md

# try to use arrow more effectively


## baseline_thp_first_cut

```
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-store$ time poetry run python scripts/migration/demo_thp_arrow_strategies.py
/GNSDATA/LIB/toshi-hazard-store/WORKING/ARROW/pq-CDC4
load ds: 0.007607, table_pandas:2.718801: filt_1: 0.484222 iter_filt_2: 0.376349
baseline_thp_first_cut took 3.7193017520476133 seconds

real    0m4.763s
```


### two more ...

this is an extremely good example , but still ....



```
/GNSDATA/LIB/toshi-hazard-store/WORKING/ARROW/pq-CDC4
load ds: 0.007536, table_pandas:1.385321: filt_1: 0.388817 iter_filt_2: 0.35966
RSS: 703MB
baseline_thp_first_cut took 2.209011 seconds

load ds: 0.000603, table_flt:0.099626: to_pandas: 0.00149 iter_filt_2: 0.37484
RSS: 0MB
more_arrow took 0.478658 seconds

(912, 3)
load ds: 0.000608, scanner:0.000164 duck_sql:0.013131: to_arrow 0.081936
RSS: 0MB
duckdb_attempt_two took 0.099231 seconds

real    0m3.839s
```

and one of the worst ....

```
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-store$ time poetry run python scripts/migration/demo_thp_arrow_strategies.py
/GNSDATA/LIB/toshi-hazard-store/WORKING/ARROW/pq-CDC4
load ds: 0.007613, table_pandas:1.295651: filt_1: 0.40045 iter_filt_2: 0.376122
RSS: 559MB
baseline_thp_first_cut took 2.132328 seconds

load ds: 0.000621, table_flt:0.671431: to_pandas: 0.006025 iter_filt_2: 0.531729
RSS: 0MB
more_arrow took 1.211358 seconds

(912, 3)
load ds: 0.000573, scanner:0.000166 duck_sql:0.026913: to_arrow 0.942266
RSS: 0MB
duckdb_attempt_two took 0.978871 seconds
```