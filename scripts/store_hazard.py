"""Script to export an openquake calculation and save it with toshi-hazard-store."""

import argparse
import datetime as dt
from pathlib import Path

try:
    from openquake.commonlib import datastore

    from toshi_hazard_store.oq_import import export_rlzs, export_rlzs_v2, export_stats, export_stats_v2
    from toshi_hazard_store.transform import export_meta
except ImportError:
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
    raise

from toshi_hazard_store import model


def extract_and_save(args):
    """Do the work."""

    hdf5_path = Path(args.calc_id)
    if hdf5_path.exists():
        # we have a file path to work with
        dstore = datastore.DataStore(str(hdf5_path))
    else:
        calc_id = int(args.calc_id)
        dstore = datastore.read(calc_id)

    toshi_id = args.toshi_id
    skip_rlzs = args.skip_rlzs

    oq = dstore['oqparam']
    R = len(dstore['full_lt'].get_realizations())

    # Save metadata record
    t0 = dt.datetime.utcnow()
    if args.verbose:
        print('Begin saving meta')
    export_meta(toshi_id, dstore, force_normalized_sites=args.force_normal_codes)
    if args.verbose:
        print("Done saving meta, took %s secs" % (dt.datetime.utcnow() - t0).total_seconds())

    if not args.new_version_only:
        # Hazard curves
        rlz_secs, agg_secs = 0, 0
        t0 = dt.datetime.utcnow()
        for kind in reversed(list(oq.get_kinds('', R))):  # do the stats curves first
            if kind.startswith('rlz-'):
                t0 = dt.datetime.utcnow()
                if skip_rlzs:
                    continue
                if args.verbose:
                    print(f'Begin saving realisations (V1) for kind {kind}')
                export_rlzs(dstore, toshi_id, kind)
                t1 = dt.datetime.utcnow()
                rlz_secs += (t1 - t0).total_seconds()
            else:
                t0 = dt.datetime.utcnow()
                if args.verbose:
                    print(f'Begin saving stats (V1) for kind {kind}')
                export_stats(dstore, toshi_id, kind)
                t1 = dt.datetime.utcnow()
                agg_secs += (t1 - t0).total_seconds()
        if args.verbose:
            print("Saving Stats curves took %s secs" % agg_secs)
            print("Saving Realization curves took %s secs" % rlz_secs)

    # new v2 stats storage
    t0 = dt.datetime.utcnow()
    if args.verbose:
        print('Begin saving stats (V2)')
    export_stats_v2(dstore, toshi_id, force_normalized_sites=args.force_normal_codes)
    if args.verbose:
        t1 = dt.datetime.utcnow()
        print("Done saving stats, took %s secs" % (t1 - t0).total_seconds())

    # new v2 realisations storage
    t0 = dt.datetime.utcnow()
    if not skip_rlzs:
        if args.verbose:
            print('Begin saving realisations (V2)')
        export_rlzs_v2(dstore, toshi_id, force_normalized_sites=args.force_normal_codes)
        if args.verbose:
            t1 = dt.datetime.utcnow()
            print("Done saving realisations, took %s secs" % (t1 - t0).total_seconds())
    elif args.verbose:
        print("Skpping saving realisations.")

    dstore.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description='store_hazard.py (store_hazard)  - extract oq hazard by calc_id and store it.'
    )
    parser.add_argument('calc_id', help='an openquake calc id OR filepath to the hdf5 file.')
    parser.add_argument('toshi_id', help='openquake_hazard_solution id.')
    parser.add_argument('-c', '--create-tables', action="store_true", help="Ensure tables exist.")
    parser.add_argument('-k', '--skip-rlzs', action="store_true", help="Skip the realizations store.")
    parser.add_argument('-v', '--verbose', help="Increase output verbosity.", action="store_true")
    parser.add_argument('-n', '--new-version-only', help="Only use the latest table version.", action="store_true")
    parser.add_argument(
        '-f',
        '--force-normal-codes',
        help="Enforce use of normalized site code. Use when the OQ job uses custom-site-id"
        "and you don't want to store those codes.",
        action="store_true",
    )
    # parser.add_argument("-s", "--summary", help="summarise output", action="store_true")
    # parser.add_argument('-D', '--debug', action="store_true", help="print debug statements")
    args = parser.parse_args()
    return args


def handle_args(args):
    # if args.debug:
    #     print(f"Args: {args}")

    if args.create_tables:
        print('Ensuring tables exist.')
        ## model.drop_tables() #DANGERMOUSE
        model.migrate()  # ensure model Table(s) exist (check env REGION, DEPLOYMENT_STAGE, etc

    extract_and_save(args)


def main():
    handle_args(parse_args())


if __name__ == '__main__':
    main()  # pragma: no cover
