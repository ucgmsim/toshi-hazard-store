# flake8: noqa
'''
This module dmemonstrates way to use pyarrow to most efficiently perform queries used in THP project.

goals are:
 - load data as fast as possible from filesystem
 - use minimum memory
 - perform aggregation computations with space.time efficiency
 - share data between different threads / processes of a compute node
 - store data effiently
'''

import inspect
import os
import pathlib
import random
import sys
import time

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from nzshm_common.grids import load_grid
from nzshm_common.location.coded_location import CodedLocation
from pyarrow import fs

nz1_grid = load_grid('NZ_0_1_NB_1_1')
partition_codes = [CodedLocation(lat=loc[0], lon=loc[1], resolution=1) for loc in nz1_grid]

CWD = pathlib.Path(os.path.realpath(__file__)).parent
ARROW_DIR = CWD.parent.parent / 'WORKING' / 'ARROW' / 'pq-CDC4'

RLZ_COUNT = 912
print(ARROW_DIR)


def baseline_thp_first_cut(loc: CodedLocation, imt="PGA", vs30=275, compat_key="A_A"):
    """
    A combination of arrow and pandas querying
    """
    filesystem = fs.LocalFileSystem()
    root = str(ARROW_DIR)

    partition = f"nloc_0={loc.downsample(1).code}"
    t0 = time.monotonic()
    dataset = ds.dataset(f'{root}/{partition}', format='parquet', filesystem=filesystem)
    t1 = time.monotonic()
    df = dataset.to_table().to_pandas()
    t2 = time.monotonic()
    ind = (
        (df['nloc_001'] == loc.downsample(0.001).code)
        & (df['imt'] == imt)
        & (df['vs30'] == vs30)
        & (df['compatible_calc_fk'] == compat_key)
    )
    df0 = df[ind]
    t3 = time.monotonic()

    for branch in range(RLZ_COUNT):  # this is NSHM count
        sources_digest = 'ef55f8757069'
        gmms_digest = 'a7d8c5d537e1'
        tic = time.perf_counter()
        ind = (df0['sources_digest'] == sources_digest) & (df0['gmms_digest'] == gmms_digest)
        df1 = df0[ind]
        if df1.shape[0] != 1:
            assert 0
    t4 = time.monotonic()

    print(
        f"load ds: {round(t1-t0, 6)}, table_pandas:{round(t2-t1, 6)}: filt_1: {round(t3-t2, 6)} iter_filt_2: {round(t4-t3, 6)}"
    )
    print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))


def more_arrow(loc: CodedLocation, imt="PGA", vs30=275, compat_key="A_A"):
    """
    Try to do more with arrow
     - get a table with only the essential cols, filtered in dataset
    """
    filesystem = fs.LocalFileSystem()
    root = str(ARROW_DIR)

    partition = f"nloc_0={loc.downsample(1).code}"
    t0 = time.monotonic()
    dataset = ds.dataset(f'{root}/{partition}', format='parquet', filesystem=filesystem)
    t1 = time.monotonic()

    flt0 = (
        (pc.field('nloc_001') == pc.scalar(loc.downsample(0.001).code))
        & (pc.field('imt') == pc.scalar(imt))
        & (pc.field('vs30') == pc.scalar(vs30))
        & (pc.field('compatible_calc_fk') == pc.scalar(compat_key))
    )
    columns = ['sources_digest', 'gmms_digest', 'values']
    table0 = dataset.to_table(columns=columns, filter=flt0)
    t2 = time.monotonic()

    # print(table0.shape)
    df0 = table0.to_pandas()
    t3 = time.monotonic()

    for branch in range(RLZ_COUNT):  # this is NSHM count
        sources_digest = 'ef55f8757069'
        gmms_digest = 'a7d8c5d537e1'
        tic = time.perf_counter()
        ind = (df0['sources_digest'] == sources_digest) & (df0['gmms_digest'] == gmms_digest)
        df1 = df0[ind]
        if df1.shape[0] != 1:
            assert 0

    t4 = time.monotonic()

    print(
        f"load ds: {round(t1-t0, 6)}, table_flt:{round(t2-t1, 6)}: to_pandas: {round(t3-t2, 6)} iter_filt_2: {round(t4-t3, 6)}"
    )

    print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))


def duckdb_wont_quack_arrow(loc: CodedLocation, imt="PGA", vs30=275, compat_key="A_A"):
    """
    introducing  out duckdb
    ref: https://duckdb.org/2021/12/03/duck-arrow.html
    """
    filesystem = fs.LocalFileSystem()
    root = str(ARROW_DIR)

    partition = f"nloc_0={loc.downsample(1).code}"
    t0 = time.monotonic()
    dataset = ds.dataset(f'{root}/{partition}', format='parquet', filesystem=filesystem)
    t1 = time.monotonic()

    # We transform the nyc dataset into a DuckDB relation
    duckie = duckdb.arrow(dataset)
    t2 = time.monotonic()

    print(duckie)
    # table = duckie.filter(f'"nloc_001" = CAST({loc.downsample(0.001).code} AS VARCHAR)')
    table = duckie.filter(f'imt = "PGA"').aggregate("SELECT PGA, COUNT(nloc_001)")

    print(table)

    # f"imt = {imt} and CAST(vs30 as DECIMAL) = {vs30} and compatible_calc_fk = {compat_key}").arrow()

    t3 = time.monotonic()
    print(table0.shape)
    df0 = table0.to_pandas()
    t4 = time.monotonic()
    for branch in range(912):  # this is NSHM count
        sources_digest = 'ef55f8757069'
        gmms_digest = 'a7d8c5d537e1'
        tic = time.perf_counter()
        ind = (df0['sources_digest'] == sources_digest) & (df0['gmms_digest'] == gmms_digest)
        df1 = df0[ind]
        if df1.shape[0] != 1:
            assert 0

    t5 = time.monotonic()

    print(
        f"load ds: {round(t1-t0, 6)}, ducked:{round(t2-t1, 6)} duck_sql:{round(t3-t2, 6)}: to_pandas: {round(t4-t3, 6)} iter_filt_2: {round(t5-t4, 6)}"
    )
    print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))


def duckdb_attempt_two(loc: CodedLocation, imt="PGA", vs30=275, compat_key="A_A"):
    """
    introducing duckdb
    ref: https://duckdb.org/docs/guides/python/sql_on_arrow
    """

    filesystem = fs.LocalFileSystem()
    root = str(ARROW_DIR)

    partition = f"nloc_0={loc.downsample(1).code}"
    t0 = time.monotonic()
    dataset = ds.dataset(f'{root}/{partition}', format='parquet', filesystem=filesystem)
    t1 = time.monotonic()

    flt0 = (
        (pc.field('nloc_001') == pc.scalar(loc.downsample(0.001).code))
        & (pc.field('imt') == pc.scalar(imt))
        & (pc.field('vs30') == pc.scalar(vs30))
        & (pc.field('compatible_calc_fk') == pc.scalar(compat_key))
    )
    columns = ['sources_digest', 'gmms_digest', 'values']
    arrow_scanner = ds.Scanner.from_dataset(dataset, filter=flt0, columns=columns)
    t2 = time.monotonic()

    con = duckdb.connect()
    results = con.execute("SELECT sources_digest, gmms_digest, values from arrow_scanner;")
    t3 = time.monotonic()
    table = results.arrow()
    print(table.shape)
    t4 = time.monotonic()
    print(
        f"load ds: {round(t1-t0, 6)}, scanner:{round(t2-t1, 6)} duck_sql:{round(t3-t2, 6)}: to_arrow {round(t4-t3, 6)}"
    )
    print("RSS: {}MB".format(pa.total_allocated_bytes() >> 20))

    return table


test_loc = random.choice(nz1_grid)
location = CodedLocation(lat=test_loc[0], lon=test_loc[1], resolution=0.001)

if __name__ == '__main__':

    t0 = time.monotonic()
    baseline_thp_first_cut(loc=location)
    t1 = time.monotonic()
    print(f"baseline_thp_first_cut took {round(t1 - t0, 6)} seconds")
    print()
    more_arrow(loc=location)
    t2 = time.monotonic()
    print(f"more_arrow took {round(t2 - t1, 6)} seconds")
    print()
    duckdb_attempt_two(location)
    t3 = time.monotonic()
    print(f"duckdb_attempt_two took {round(t3 - t2, 6)} seconds")
    # print("LOCAL dataset partition tests")
