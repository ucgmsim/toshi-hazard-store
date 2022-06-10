"""Script to query toshi-hazard-store."""

import argparse
import datetime as dt

from toshi_hazard_store import query


def run_vs30_query(vs30_vals):
    """Do the work."""

    ## get some solution meta data ...
    for m in query.get_hazard_metadata(None, vs30_vals=vs30_vals):  # get all solution meta with given VS values
        print(m.vs30, m.haz_sol_id, m.locs)


def run_stats_query(args):

    fn = query.get_hazard_stats_curves_v2 if args.new_version else query.get_hazard_stats_curves

    tids = args.toshi_ids.split(',')
    imts = args.imts.split(',') if args.imts else None
    locs = args.locs.split(',') if args.locs else None
    aggs = args.aggs.split(',') if args.aggs else None
    t0 = dt.datetime.utcnow()
    cnt = 0
    for tid in tids:
        res = fn(tid, imts=imts, locs=locs, aggs=aggs)
        for r in res:
            if args.new_version:
                for v in r.values:
                    if cnt == 0:
                        print(
                            'loc',
                            'agg',
                            'imt',
                        )  # ",".join([str(l) for l in v.lvls]))
                    print(
                        r.loc,
                        r.agg,
                    )  # v.imt, #",".join([str(vl) for vl in v.vals]))
                    cnt += 1
            else:
                print(
                    r.loc,
                    r.agg,
                )
                cnt += 1

    print(cnt, "Took %s secs" % (dt.datetime.utcnow() - t0).total_seconds())


def parse_args():
    parser = argparse.ArgumentParser(
        description='store_hazard.py (store_hazard)  - extract oq hazard by calc_id and store it.'
    )
    parser.add_argument('toshi_ids', help='list of openquake_hazard_solution ids.')
    parser.add_argument('--locs', help='list of location IDs (comma-separated')
    parser.add_argument('--vs30s', help='list of location IDs (comma-separated')

    parser.add_argument('--imts', help='list of IMT (comma-separated')
    parser.add_argument('--rlzs', help='list of location IDs (comma-separated)')
    parser.add_argument('--aggs', help='list of aggs (comma-separated) e.g. "mean,0.1,0.5"')
    parser.add_argument('-v', '--verbose', help="Increase output verbosity.", action="store_true")
    parser.add_argument('-n', '--new-version', help="Use the latest table version.", action="store_true")
    # parser.add_argument("-s", "--summary", help="summarise output", action="store_true")
    # parser.add_argument('-D', '--debug', action="store_true", help="print debug statements")
    args = parser.parse_args()
    return args


def handle_args(args):
    if args.vs30s:
        vs30s = [int(v) for v in args.vs30s.split(',')]
        print(f'Getting IDs where vs30 in {vs30s}')
        run_vs30_query(vs30s)

    if args.aggs:
        print(f'Getting stats with {args.aggs}')
        run_stats_query(args)


def main():
    handle_args(parse_args())


if __name__ == '__main__':
    main()  # pragma: no cover
