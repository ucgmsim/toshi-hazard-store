"""Script to query toshi-hazard-store."""

import argparse
import datetime as dt

from toshi_hazard_store.query_v3 import get_rlz_curves_v3


def run_query(args):

    print(args)

    locs = args.locs.split(',')
    locs = [loc.replace('\\', '') for loc in locs]
    tids = [tid.strip() for tid in args.toshi_ids.split(',')] if args.toshi_ids else None
    vs30s = [int(v) for v in args.vs30s.split(',')] if args.vs30s else None
    imts = args.imts.split(',') if args.imts else None
    rlzs = [int(r) for r in args.rlzs.split(',')] if args.rlzs else None

    t0 = dt.datetime.utcnow()
    cnt = 0

    for res in get_rlz_curves_v3(locs, vs30s, rlzs, tids, imts):
        if args.verbose:
            print(res)
            print(res, res.created, res.source_tags, res.source_ids)
        cnt += 1

    print(cnt, "Took %s secs" % (dt.datetime.utcnow() - t0).total_seconds())


def parse_args():
    parser = argparse.ArgumentParser(
        description='store_hazard.py (store_hazard)  - extract oq hazard by calc_id and store it.'
    )
    parser.add_argument('locs', help='list of location IDs (csv-like \\-46.800~168.800"')
    parser.add_argument('--toshi_ids', help='list of toshi ids.')
    parser.add_argument('--vs30s', help='list of location IDs (comma-separated')
    parser.add_argument('--imts', help='list of IMT (comma-separated')
    parser.add_argument('--rlzs', help='list of location IDs (comma-separated)')

    parser.add_argument('-v', '--verbose', help="Increase output verbosity.", action="store_true")
    parser.add_argument('-n', '--new-version', help="Use the latest table version.", action="store_true")
    args = parser.parse_args()
    return args


def main():
    run_query(parse_args())


if __name__ == '__main__':
    main()  # pragma: no cover
