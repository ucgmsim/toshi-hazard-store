"""Script to query toshi-hazard-store."""

import collections
import json

from toshi_hazard_store.query_v3 import get_rlz_curves_v3

HazardRlz = collections.namedtuple('HazardRlz', 'nloc_001 tid vs30 rlz imt values tags ids')


def main():

    locs = [
        "-43.530~172.630",
    ]
    tids = ["A_CRU", "A_PUY", "A_HIK"]
    vs30s = [750]
    imts = ['PGA']
    rlzs = None

    cnt = 0

    output = []
    for res in get_rlz_curves_v3(locs, vs30s, rlzs, tids, imts):
        # print(res)
        # print( res, res.created, res.source_tags, res.source_ids)
        values = res.values
        for rec in values:
            # print( rec.imt, rec.vals )
            h = HazardRlz(
                res.nloc_001,
                res.hazard_solution_id,
                res.vs30,
                res.rlz,
                rec.imt,
                rec.vals,
                list(res.source_tags),
                list(res.source_ids),
            )
            output.append(h._asdict())
            # print(h._asdict())
        cnt += 1

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
