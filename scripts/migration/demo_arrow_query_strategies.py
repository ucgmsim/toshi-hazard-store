# flake8: noqa
"""
test performance of a few key arrow queries - initially for THP
"""

import inspect
import os
import pathlib
import random
import sys
import time
from typing import List, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from nzshm_common import location
from nzshm_common.grids import load_grid
from nzshm_common.location.coded_location import CodedLocation
from pyarrow import fs

nz1_grid = load_grid('NZ_0_1_NB_1_1')
# city_locs = [(location.LOCATIONS_BY_ID[key]['latitude'], location.LOCATIONS_BY_ID[key]['longitude'])
#     for key in location.LOCATION_LISTS["NZ"]["locations"]]
# srwg_locs = [(location.LOCATIONS_BY_ID[key]['latitude'], location.LOCATIONS_BY_ID[key]['longitude'])
#     for key in location.LOCATION_LISTS["SRWG214"]["locations"]]
# all_locs = set(nz1_grid + srwg_locs + city_locs)

partition_codes = [CodedLocation(lat=loc[0], lon=loc[1], resolution=1) for loc in nz1_grid]

CWD = pathlib.Path(os.path.realpath(__file__)).parent
ARROW_DIR = CWD.parent.parent / 'WORKING' / 'ARROW'


class TimedDatasetTests:

    def __init__(self, source: str, dataset_name: str, test_locations, partition=False):
        assert source in ["S3", "LOCAL"]
        self.source = source
        self.dataset_name = dataset_name
        self.test_locations = test_locations
        self._timing_log: List[Tuple] = []
        self.partition = self._random_partition().code if partition else None

    def _random_partition(self):
        loc0 = random.choice(self.test_locations)
        return loc0.resample(1)

    def random_new_location(self):
        """Choose a random location, get it's partioning, then choose test
        locations within that partion"""
        if self.partition:
            # partition = self._random_partition()
            test_locations = []
            for loc in self.test_locations:
                if loc.resample(1).code == self.partition:
                    test_locations.append(loc)
                self._current_test_locations = test_locations
            self.test_location = random.choice(self._current_test_locations)
            # self.partition = partition.code
        else:
            self.test_location = random.choice(self.test_locations)

    def _open_dataset(self) -> ds:
        if self.source == 'S3':
            filesystem = fs.S3FileSystem(region='ap-southeast-2')
            root = 'ths-poc-arrow-test'
        else:
            root = str(ARROW_DIR)
            filesystem = fs.LocalFileSystem()
        if self.partition:
            return ds.dataset(
                f'{root}/{self.dataset_name}/nloc_0={self.partition}', format='parquet', filesystem=filesystem
            )
        else:
            return ds.dataset(f'{root}/{self.dataset_name}', format='parquet', filesystem=filesystem)

    def log_timing(self, fn_name, elapsed_time, fn_args=None):
        self._timing_log.append((fn_name, fn_args, elapsed_time))

    def report_timings(self):
        # print(self._timing_log)
        for log_itm in self._timing_log:
            if log_itm[1]:
                yield f"{log_itm[0]} with ({log_itm[1]}) took: {round(log_itm[2], 6)} seconds"
            else:
                yield f"{log_itm[0]} took: {round(log_itm[2], 6)} seconds"

    def time_open_dataset(self):
        self.random_new_location()
        t0 = time.monotonic()
        dataset = self._open_dataset()  #
        elapsed_time = time.monotonic() - t0
        fn = inspect.currentframe().f_code.co_name
        self.log_timing(
            fn,
            elapsed_time,
            self.partition,
        )

    def time_query_df_one_location(self):
        t0 = time.monotonic()
        self.random_new_location()
        dataset = self._open_dataset()
        flt = (pc.field('imt') == pc.scalar("PGA")) & (pc.field("nloc_001") == pc.scalar(self.test_location.code))
        df = dataset.to_table(filter=flt).to_pandas()
        # hazard_calc_ids = list(df.calculation_id.unique())
        elapsed_time = time.monotonic() - t0
        fn = inspect.currentframe().f_code.co_name
        self.log_timing(
            fn,
            elapsed_time,
            self.partition,
        )

    def time_query_many_locations_naive(self, count=2):
        t0 = time.monotonic()
        tr = 0
        for test in range(count):
            t1 = time.monotonic()
            self.random_new_location()
            tr += time.monotonic() - t1
            dataset = self._open_dataset()
            flt = (pc.field('imt') == pc.scalar("PGA")) & (pc.field("nloc_001") == pc.scalar(self.test_location.code))
            df = dataset.to_table(filter=flt).to_pandas()
            assert df.shape[0] == 912

        # hazard_calc_ids = list(df.calculation_id.unique())
        elapsed_time = time.monotonic() - t0
        fn = inspect.currentframe().f_code.co_name
        self.log_timing(fn, elapsed_time - tr, f"{count} locations")

    def time_query_many_locations_better(self, count):
        t0 = time.monotonic()
        tr = 0
        dataset = self._open_dataset()
        for test in range(count):
            t1 = time.monotonic()
            self.random_new_location()
            tr += time.monotonic() - t1
            flt = (pc.field('imt') == pc.scalar("PGA")) & (pc.field("nloc_001") == pc.scalar(self.test_location.code))
            df = dataset.to_table(filter=flt).to_pandas()
            assert df.shape[0] == 912

        # hazard_calc_ids = list(df.calculation_id.unique())
        elapsed_time = time.monotonic() - t0
        fn = inspect.currentframe().f_code.co_name
        self.log_timing(fn, elapsed_time - tr, f"{count} locations")

    def time_query_many_locations_better_again(self, count):
        t0 = time.monotonic()
        tr = 0
        dataset = self._open_dataset()
        df = dataset.to_table().to_pandas()  # filter=(pc.field('imt') == pc.scalar("SA(0.5)")
        for test in range(count):

            t1 = time.monotonic()
            self.random_new_location()
            tr += time.monotonic() - t1

            # now filter using pandas...
            df0 = df[(df.nloc_001 == self.test_location.code) & (df.imt == "PGA")]
            # print(df0)
            if not df0.shape[0] == 912:
                print(df0)
                assert 0

        # hazard_calc_ids = list(df.calculation_id.unique())
        elapsed_time = time.monotonic() - t0
        fn = inspect.currentframe().f_code.co_name
        self.log_timing(fn, elapsed_time - tr, f"{count} locations")

    def run_timings(self):
        self.time_open_dataset()
        # self.time_query_df_one_location()
        # if self.partition:
        #     self.time_query_many_locations_naive(2)
        #     self.time_query_many_locations_better(10)
        self.time_query_many_locations_better_again(10)
        if self.partition:
            # self.time_query_many_locations_better_again(50)
            self.time_query_many_locations_better_again(100)
        return self


if __name__ == '__main__':

    # partition = random.choice(partition_codes)
    # tloc = random.choice(list(all_locs))
    test_locations = [CodedLocation(lat=loc[0], lon=loc[1], resolution=0.001) for loc in nz1_grid]
    partition = random.choice(partition_codes)

    print("LOCAL dataset partition tests")
    test0 = TimedDatasetTests("LOCAL", 'pq-CDC2', test_locations, partition=True)
    test0.time_query_many_locations_better(10)
    test0.time_query_df_one_location()

    # .run_timings()
    for report in test0.report_timings():
        print(report)
    print()

    # print("LOCAL top level dataset tests")
    # test0 = TimedDatasetTests("LOCAL", 'pq-CDC2', test_locations).run_timings()
    # for report in test0.report_timings():
    #     print(report)

    # print("AWS S3 dataset partition tests")
    # test0 = TimedDatasetTests("S3", 'pq-CDC2', test_locations, partition=True).run_timings()
    # for report in test0.report_timings():
    #     print(report)

    # print(f"open local dataset (one VS30): {time_open_entire_dataset()}")
    # print(f"open local dataset partition (one VS30, {partition.code}): {time_open_dataset_partition(partition)}")

    # print(f"dataset full/partition (one VS30) {time_open_entire_dataset()/time_open_dataset_partition(partition)}")
