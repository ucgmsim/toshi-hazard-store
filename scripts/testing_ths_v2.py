import logging
import os
import subprocess
from pathlib import Path

from nzshm_common.location.coded_location import CodedLocation
from nzshm_common.location.location import location_by_id

import toshi_hazard_store

log = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)


def get_locations(locations):
    def lat_lon(_id):
        return (location_by_id(_id)['latitude'], location_by_id(_id)['longitude'])

    return [CodedLocation(*lat_lon(loc), 0.001).code for loc in locations]


# ths_sqlite_folder = "/home/chrisdc/.cache/toshi_hazard_store"
vs30 = 400
imts = ['PGA', 'SA(0.5)', 'SA(1.5)', 'SA(3.0)']
locations = ["WLG", "DUD", "CHC", "AKL"]

STAGE = "TEST_CBC"

loc_codes = get_locations(locations)


def save_rlz(hdf5_path, haz_id, use_sql):
    my_env = os.environ.copy()
    my_env["NZSHM22_HAZARD_STORE_STAGE"] = STAGE
    cmd_v3 = ["store_hazard_v3", hdf5_path, haz_id, "DUMMY", "DUMMY", "DUMMY", "DUMMY", "--verbose", "--create-tables"]
    cmd_v4 = [
        "python3",
        "scripts/store_hazard_v4.py",
        "--calc-id",
        hdf5_path,
        "--compatible-calc-fk",
        haz_id,
        "--producer-config-fk",
        haz_id,
        "--verbose",
        "--create-tables",
    ]

    cmd = cmd_v4

    if use_sql:
        my_env["THS_SQLITE_FOLDER"] = ths_sqlite_folder
        my_env["THS_USE_SQLITE_ADAPTER"] = "TRUE"
    else:
        my_env["THS_USE_SQLITE_ADAPTER"] = "FALSE"
    print(cmd)
    subprocess.run(cmd, env=my_env)
    # subprocess.run(cmd)


def load_rlz(haz_id, use_sql):
    # os.environ["NZSHM22_HAZARD_STORE_STAGE"] = STAGE
    # if use_sql:
    #     os.environ["THS_SQLITE_FOLDER"] = ths_sqlite_folder
    #     os.environ["THS_USE_SQLITE_ADAPTER"] = "TRUE"
    # else:
    #     os.environ["THS_USE_SQLITE_ADAPTER"] = "FALSE"
    for i, res in enumerate(
        toshi_hazard_store.query_v3.get_rlz_curves_v3(loc_codes, [vs30], list(range(21)), [haz_id], imts)
    ):
        print(i, res.hazard_solution_id, res.nloc_001)


#####################################################################
# oqdata_path = Path("/home/chrisdc/oqdata")
oqdata_path = Path("/Users/chrisbc/DEV/GNS/toshi-hazard-store/LOCALSTORAGE/test_hdf5")

hdf5_files = ["calc_38.hdf5", "calc_39.hdf5", "calc_40.hdf5"]
haz_ids = ["calc_38", "calc_39", "calc_40"]
# calc_fks
# producer_fks
hazard_suffix = "a"

# for usesql in [True, False]:
#     print(f"Using SQLITE: {usesql}\n")
#     for hdf5, hazid in zip(hdf5_files, haz_ids):
#         save_rlz(str(oqdata_path / hdf5), hazid + hazard_suffix, usesql)

# for usesql in [True, False]:
#     print("")
#     print('=' * 50)
#     print(f"Using SQLITE: {usesql}")

#     for hazid in haz_ids:
#         load_rlz(hazid + hazard_suffix, usesql)

# for hazid in haz_ids:
#     load_rlz(hazid + hazard_suffix, False)

for hdf5, hazid in zip(hdf5_files, haz_ids):
    save_rlz(str(oqdata_path / hdf5), hazid + hazard_suffix, use_sql=False)
