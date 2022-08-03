"""Script to aggregate realisations from toshi-hazard-store."""

import linecache
import os
import time
import tracemalloc  # see https://stackoverflow.com/questions/552744/how-do-i-profile-memory-usage-in-python

import numpy as np
from nz_binned_demo import locations_nzpt2_and_nz34_binned
from SLT_37_GRANULAR_RELEASE_1 import logic_tree_permutations
from SLT_37_GT import merge_ltbs

from toshi_hazard_store import aggregate_rlzs as ag
from toshi_hazard_store.query_v3 import get_rlz_curves_v3


def display_top(snapshot, key_type='lineno', limit=3):
    snapshot = snapshot.filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        )
    )
    top_stats = snapshot.statistics(key_type)

    print("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        # replace "/path/to/module/file.py" with "module/file.py"
        filename = os.sep.join(frame.filename.split(os.sep)[-2:])
        print("#%s: %s:%s: %.1f KiB" % (index, filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print('    %s' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))


# #some use of nzshm_common -  needs improvement
# from nzshm_common.grids.region_grid import load_grid
# from nzshm_common.location.location import LOCATIONS_BY_ID
# from nzshm_common.location.code_location import CodedLocation
# from typing import List, Tuple, Dict

# def locations_by_degree(grid_points:List[Tuple[float,float]], grid_res:float, point_res:float) -> Dict[str, List[str]]: # noqa
#     """Produce a dict of key_location:"""
#     binned = dict()
#     for pt in grid_points:
#         bc = CodedLocation(*pt).downsample(grid_res).code
#         if not binned.get(bc):
#             binned[bc] = []
#         binned[bc].append(CodedLocation(*pt).downsample(point_res).code)
#     return binned

# def locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001):

#     # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
#     nz_0_2 = load_grid("NZ_0_2_NB_1_1")
#     nz34 = [(o['latitude'], o['longitude']) for o in LOCATIONS_BY_ID.values()]

#     grid_points = nz34 + nz_0_2

#     # For NZ_0_2: binning 1.0 =>  66 bins max 25 pts
#     # For NZ 0_2: binning 0.5 => 202 bins max 9 pts
#     return locations_by_degree(grid_points, grid_res, point_res).items()


def process():

    # tracemalloc.start()
    tic_total = time.perf_counter()

    # TODO: I'm making assumptions that the levels array is the same for every realization, imt, run, etc.
    # If that were not the case, I would have to add some interpolation

    loc = "-43.530~172.630"  # CHC
    vs30 = 750
    # rlzs = None
    agg = 'mean'
    # agg = [0.5]

    source_branches = [
        dict(name='A', ids=['A_CRU', 'A_HIK', 'A_PUY'], weight=0.25),
        dict(name='B', ids=['B_CRU', 'B_HIK', 'B_PUY'], weight=0.75),
    ]

    imts = ag.get_imts(source_branches, vs30)

    values = ag.cache_realization_values(source_branches, loc, vs30)

    toc_total = time.perf_counter()
    print(f'time to fetch rlzs: {toc_total-tic_total:.1f} seconds')

    median = {}
    for imt in imts:
        weights, branch_probs = ag.build_branches(source_branches, values, imt, vs30)
        median[imt] = ag.calculate_agg(branch_probs, agg, weights)  # TODO: could be stored in pandas DataFrame

    toc_total = time.perf_counter()
    print(f'total time: {toc_total-tic_total:.1f} seconds')

    for k, v in median.items():
        print('=' * 50)
        print(f'{k}:')
        print(v[:4], '...', v[-4:])

    # snapshot = tracemalloc.take_snapshot()
    # display_top(snapshot)


if __name__ == "__main__":
    omit = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2MDEy']  # this is the failed/clonded job
    # for ltb in merge_ltbs(logic_tree_permutations, omit):
    #     print(ltb)

    # # Settings for the THS rlz_query
    # for key, locs in locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001):
    #     print(f"{key} => {locs}")

    t0 = time.perf_counter()
    values = {}

    # branches = list(merge_ltbs(logic_tree_permutations, omit))
    toshi_ids = [b.hazard_solution_id for b in merge_ltbs(logic_tree_permutations, omit)]
    print(len(set(toshi_ids)))
    for key, locs in locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001):
        print(f'get values for {len(locs)} locations and {len(toshi_ids)} hazard_solutions')
        # print(f'locs {locs}')
        for res in get_rlz_curves_v3(locs, [750], None, toshi_ids, None):
            key = ':'.join((res.hazard_solution_id, str(res.rlz)))
            values[key] = {}
            for val in res.values:
                values[key][val.imt] = np.array(val.vals)
        # print(values)
        t1 = time.perf_counter()
        print(f'total time: {t1-t0:.1f} seconds')
        t0 = t1
    assert 0

    process()
