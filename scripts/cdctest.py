import logging
import sys

from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import location_by_id

import toshi_hazard_store

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)

# if USE_SQLITE_ADAPTER:
#     print("CONFIGURING")
#     configure_adapter(adapter_model=SqliteAdapter)

locations = ["WLG", "DUD", "CHC", "AKL"]
# hazard_ids = ["T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTkyMg=="]
# hazard_ids = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTkyOQ==', 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTkzMQ==',
# 'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTkzMw==']

# local
# hazard_ids = ["ABC4"]

# cloud
# hazard_ids = ["T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTk0Mw==", "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTk0NQ==",
# "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTk0Nw=="]

# cloud 2
hazard_ids = [
    "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTk1MA==",
    "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTk1Mg==",
    "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246NjgwMTk1NA==",
]

hazard_ids = ["ABC1"]
vs30 = 400
imts = ['PGA', 'SA(0.5)', 'SA(1.5)', 'SA(3.0)']


def get_locations(locations):
    def lat_lon(_id):
        return (location_by_id(_id)['latitude'], location_by_id(_id)['longitude'])

    return [CodedLocation(*lat_lon(loc), 0.001).code for loc in locations]


loc_codes = get_locations(locations)

for res in toshi_hazard_store.query_v3.get_rlz_curves_v3(loc_codes, [vs30], list(range(21)), hazard_ids, imts):
    imts = [val.imt for val in res.values]
    print(res.hazard_solution_id, res.nloc_001, imts)  # , res)
    # print(res.values[0].vals)
print("All  Done")
