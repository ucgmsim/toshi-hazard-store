"""nzshm_common building some lists for aggregations."""
from typing import Dict, List, Tuple

from nzshm_common.grids.region_grid import load_grid
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID


def locations_by_degree(
    grid_points: List[Tuple[float, float]], grid_res: float, point_res: float
) -> Dict[str, List[str]]:
    """Produce a dict of key_location:"""
    binned = dict()
    for pt in grid_points:
        bc = CodedLocation(*pt).downsample(grid_res).code
        if not binned.get(bc):
            binned[bc] = []
        binned[bc].append(CodedLocation(*pt).downsample(point_res).code)
    return binned


def locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001):

    # wlg_grid_0_01 = load_grid("WLG_0_01_nb_1_1")
    nz_0_2 = load_grid("NZ_0_2_NB_1_1")
    nz34 = [(o['latitude'], o['longitude']) for o in LOCATIONS_BY_ID.values()]

    grid_points = nz34 + nz_0_2

    return locations_by_degree(grid_points, grid_res, point_res).items()


if __name__ == "__main__":

    # For NZ_0_2: binning 1.0 =>  66 bins max 25 pts
    # For NZ 0_2: binning 0.5 => 202 bins max 9 pts

    # Settings for the THS rlz_query
    for key, locs in locations_nzpt2_and_nz34_binned(grid_res=1.0, point_res=0.001):
        print(f"{key} => {locs}")
