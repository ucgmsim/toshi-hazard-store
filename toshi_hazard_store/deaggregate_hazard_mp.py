import logging
import multiprocessing
import time
from collections import namedtuple
from pathlib import Path

# add location code for sites that have them
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID

from toshi_hazard_store.branch_combinator.SLT_TAG_FINAL import data as gtdata
from toshi_hazard_store.branch_combinator.SLT_TAG_FINAL import logic_tree_permutations

# from toshi_hazard_store.aggregate_rlzs import (
#     #build_rlz_table,
#     concat_df_files,
#     #get_imts,
#     #get_levels,
#     #process_disagg_location_list,
#     #process_location_list,
# )
# from toshi_hazard_store.branch_combinator.branch_combinator import (
#     get_weighted_branches,
#     grouped_ltbs,
#     merge_ltbs_fromLT,
# )


class DisaggHardWorker(multiprocessing.Process):
    """A worker that batches and saves records to DynamoDB.

    based on https://pymotw.com/2/multiprocessing/communication.html example 2.
    """

    def __init__(self, task_queue, result_queue):
        multiprocessing.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        print(f"worker {self.name} running.")
        proc_name = self.name

        while True:
            nt = self.task_queue.get()
            if nt is None:
                # Poison pill means shutdown
                self.task_queue.task_done()
                print('%s: Exiting' % proc_name)
                break

            # tic = time.perf_counter()
            disagg_configs = process_disagg_location_list(
                nt.hazard_curves,
                nt.source_branches,
                nt.toshi_ids,
                nt.poes,
                nt.inv_time,
                nt.vs30,
                nt.locs,
                nt.aggs,
                nt.imts,
            )
            self.task_queue.task_done()
            self.result_queue.put(disagg_configs)


DisaggTaskArgs = namedtuple(
    "DisaggTaskArgs", "grid_loc hazard_curves source_branches toshi_ids poes inv_time vs30 locs aggs imts"
)


def process_disaggs(
    hazard_curves, source_branches, poes, inv_time, vs30, location_generator, aggs, imts, num_workers=12
):

    # write serial code for now, parallelize once it works
    omit = []
    toshi_ids = [b.hazard_solution_id for b in merge_ltbs_fromLT(logic_tree_permutations, gtdata=gtdata, omit=omit)]

    task_queue: multiprocessing.JoinableQueue = multiprocessing.JoinableQueue()
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    print('Starting Disaggregations')
    print('Creating %d workers' % num_workers)
    workers = [DisaggHardWorker(task_queue, result_queue) for i in range(num_workers)]
    for w in workers:
        w.start()

    tic = time.perf_counter()
    # Enqueue jobs
    num_jobs = 0
    for key, locs in location_generator().items():
        print(locs)
        t = DisaggTaskArgs(key, hazard_curves, source_branches, toshi_ids, poes, inv_time, vs30, locs, aggs, imts)
        task_queue.put(t)
        num_jobs += 1

    # Add a poison pill for each to signal we've done everything
    for i in range(num_workers):
        task_queue.put(None)

    # Wait for all of the tasks to finish
    task_queue.join()

    # Start printing results
    print('Results:')
    disagg_configs = []
    while num_jobs:
        result = result_queue.get()
        disagg_configs += result
        print(str(result))
        num_jobs -= 1

    toc = time.perf_counter()
    print(f'time to run disaggregations: {toc-tic:.0f} seconds')

    return disagg_configs

    # ========================================#

    # disagg_configs = []
    # for key, locs in location_generator().items():
    #     disagg_configs += process_disagg_location_list(hazard_curves, source_branches, toshi_ids,
    #         poes, inv_time, vs30, locs, aggs, imts)
    # return disagg_configs


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    logging.getLogger('pynamodb').setLevel(logging.DEBUG)
    logging.getLogger('toshi_hazard_store').setLevel(logging.DEBUG)

    nproc = 20

    # # if running classical and disagg you must make sure that the requested locations, imts, vs30, aggs for disaggs
    # # are in what was requested for the classical calculation
    disaggs = False
    poes = [0.1, 0.02]
    aggs = ['mean']
    inv_time = 50
    imts = ['PGA', 'SA(0.5)', 'SA(1.5)']
    # output_prefix = 'fullLT_dissags'
    # location_generator = locations_nz34_chunked
    # # location_generator = locations_nz2_chunked
    # # breakpoint()

    disagg_configs = process_disaggs(
        hazard_curves, source_branches, poes, inv_time, vs30, location_generator, aggs, imts, num_workers=nproc
    )

    for disagg_config in disagg_configs:
        for loc in LOCATIONS_BY_ID.values():
            location = CodedLocation(loc['latitude'], loc['longitude']).downsample(0.001).code
            if location == disagg_config['location']:
                disagg_config['site_code'] = loc['id']
                disagg_config['site_name'] = loc['name']

    with open('disagg_configs.json', 'w') as json_file:
        json.dump(disagg_configs, json_file)
