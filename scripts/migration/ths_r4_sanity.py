# flake8: noqa
"""
Console script for querying tables before and after import/migration to ensure that we have what we expect
"""

import importlib
import itertools
import json
import logging
import pathlib
import random

import click
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
# logging.getLogger('pynamodb').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('toshi_hazard_store').setLevel(logging.WARNING)
# logging.getLogger('toshi_hazard_store.db_adapter.sqlite.pynamodb_sql').setLevel(logging.DEBUG)

from nzshm_common import location
from nzshm_common.grids import load_grid
from nzshm_common.location.coded_location import CodedLocation
from pynamodb.models import Model

import toshi_hazard_store  # noqa: E402
import toshi_hazard_store.config
import toshi_hazard_store.model.openquake_models
import toshi_hazard_store.model.revision_4.hazard_models  # noqa: E402
import toshi_hazard_store.query.hazard_query
from scripts.core import echo_settings  # noqa
from toshi_hazard_store.config import DEPLOYMENT_STAGE as THS_STAGE
from toshi_hazard_store.config import USE_SQLITE_ADAPTER  # noqa
from toshi_hazard_store.config import LOCAL_CACHE_FOLDER
from toshi_hazard_store.config import REGION as THS_REGION
from toshi_hazard_store.db_adapter.dynamic_base_class import ensure_class_bases_begin_with, set_base_class
from toshi_hazard_store.db_adapter.sqlite import (  # noqa this is needed to finish the randon-rlz functionality
    SqliteAdapter,
)

nz1_grid = load_grid('NZ_0_1_NB_1_1')
city_locs = [(location.LOCATIONS_BY_ID[key]['latitude'], location.LOCATIONS_BY_ID[key]['longitude'])
    for key in location.LOCATION_LISTS["NZ"]["locations"]]
srwg_locs = [(location.LOCATIONS_BY_ID[key]['latitude'], location.LOCATIONS_BY_ID[key]['longitude'])
    for key in location.LOCATION_LISTS["SRWG214"]["locations"]]

all_locs = set(nz1_grid + srwg_locs + city_locs)

# print(nz1_grid[:10])
# print(srwg_locs[:10])
# print(city_locs[:10])


def get_random_args(gt_info, how_many):
    for n in range(how_many):
        yield dict(
            tid=random.choice(
                [
                    edge['node']['child']["hazard_solution"]["id"]
                    for edge in gt_info['data']['node']['children']['edges']
                ]
            ),
            rlz=random.choice(range(20)),
            locs=[CodedLocation(o[0], o[1], 0.001) for o in random.sample(nz1_grid, how_many)],
        )


def query_table(args):
    # mRLZ = toshi_hazard_store.model.openquake_models.__dict__['OpenquakeRealization']
    importlib.reload(toshi_hazard_store.query.hazard_query)
    for res in toshi_hazard_store.query.hazard_query.get_rlz_curves_v3(
        locs=[loc.code for loc in args['locs']], vs30s=[275], rlzs=[args['rlz']], tids=[args['tid']], imts=['PGA']
    ):
        yield (res)


def get_table_rows(random_args_list):
    result = {}
    for args in random_args_list:
        for res in query_table(args):
            obj = res.to_simple_dict(force=True)
            result[obj["sort_key"]] = obj
    return result


def report_arrow_count_loc_rlzs(ds_name, location, verbose):
    """report on dataset realisations for a singel location"""
    dataset = ds.dataset(f'./WORKING/ARROW/{ds_name}/nloc_0={location.resample(1).code}', format='parquet')

    click.echo(f"querying arrow/parquet dataset {dataset}")
    flt = (pc.field('imt') == pc.scalar("PGA")) & (pc.field("nloc_001") == pc.scalar(location.code))
    # flt = pc.field("nloc_001")==pc.scalar(location.code)
    df = dataset.to_table(filter=flt).to_pandas()

    # get the unique hazard_calcluation ids...
    hazard_calc_ids = list(df.calculation_id.unique())

    if verbose:
        click.echo(hazard_calc_ids)
        click.echo
    count_all = 0
    for calc_id in hazard_calc_ids:
        df0 = df[df.calculation_id == calc_id]
        click.echo(f"-42.450~171.210, {calc_id}, {df0.shape[0]}")
        count_all += df0.shape[0]
    click.echo()
    click.echo(f"Grand total: {count_all}")

def report_v3_count_loc_rlzs(location, verbose):
    #### MONKEYPATCH ...
    # toshi_hazard_store.config.REGION = "ap-southeast-2"
    # toshi_hazard_store.config.DEPLOYMENT_STAGE = "PROD"
    # importlib.reload(toshi_hazard_store.model.openquake_models)
    ####
    mRLZ = toshi_hazard_store.model.openquake_models.OpenquakeRealization

    gtfile = pathlib.Path(__file__).parent / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
    gt_info = json.load(open(str(gtfile)))
    tids = [edge['node']['child']['hazard_solution']["id"] for edge in gt_info['data']['node']['children']['edges']]

    if verbose:
        click.echo(tids)
        click.echo()
    count_all = 0

    for tid in tids:
        rlz_count = mRLZ.count(
            location.resample(0.1).code,
            mRLZ.sort_key >= f'{location.code}:275:000000:{tid}',
            filter_condition=(mRLZ.nloc_001 == location.code) & (mRLZ.hazard_solution_id == tid),
        )
        count_all += rlz_count
        click.echo(f"{location.code}, {tid}, {rlz_count}")

    click.echo()
    click.echo(f"Grand total: {count_all}")
    return


# report_row = namedtuple("ReportRow", "task-id, uniq_locs, uniq_imts, uniq_gmms, uniq_srcs, uniq_vs30s, consistent)")

def report_rlzs_grouped_by_calc(ds_name, verbose, bail_on_error=True):
    """report on dataset realisations"""
    dataset = ds.dataset(f'./WORKING/ARROW/{ds_name}',
            partitioning='hive')
            #, format='arrow')
    click.echo(f"querying arrow/parquet dataset {ds_name}")
    loc = CodedLocation(lat=-46, lon=169.5, resolution=0.001)
    fltA = (pc.field('imt') == pc.scalar("PGA")) & (pc.field("nloc_001") == pc.scalar(loc.code))
    df = dataset.to_table(filter=fltA).to_pandas()
    hazard_calc_ids = list(df.calculation_id.unique())
    count_all = 0
    click.echo("calculation_id, uniq_rlzs, uniq_locs, uniq_imts, uniq_gmms, uniq_srcs, uniq_vs30, consistent")
    click.echo("============================================================================================")
    for calc_id in sorted(hazard_calc_ids):
        flt = pc.field('calculation_id') == pc.scalar(calc_id)
        df0 = dataset.to_table(filter=flt).to_pandas()
        uniq_locs = len(list(df0.nloc_001.unique()))
        uniq_imts = len(list(df0.imt.unique()))
        uniq_gmms = len(list(df0.gmms_digest.unique()))
        uniq_srcs = len(list(df0.sources_digest.unique()))
        uniq_vs30 = len(list(df0.vs30.unique()))
        consistent = (uniq_locs * uniq_imts * uniq_gmms * uniq_srcs * uniq_vs30) == df0.shape[0]
        click.echo(
            f"{calc_id}, {df0.shape[0]}, {uniq_locs}, {uniq_imts}, {uniq_gmms}, {uniq_srcs}, {uniq_vs30}, {consistent}"
        )
        count_all += df0.shape[0]

        if bail_on_error and not consistent:
            return

    click.echo()
    click.echo(f"Grand total: {count_all}")

    if verbose:
        click.echo()
        click.echo(df0)


def report_v3_grouped_by_calc(verbose, bail_on_error=True):
    """report on dataset realisations"""
    mRLZ = toshi_hazard_store.model.openquake_models.OpenquakeRealization

    gtfile = pathlib.Path(__file__).parent / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
    gt_info = json.load(open(str(gtfile)))
    calc_ids = [edge['node']['child']['hazard_solution']["id"] for edge in gt_info['data']['node']['children']['edges']]

    all_partitions = set([CodedLocation(lat=loc[0], lon=loc[1], resolution=0.1) for loc in list(all_locs)])
    if verbose:
        click.echo("Calc IDs")
        click.echo(calc_ids)
        click.echo()
        click.echo("Location Partitions")
        click.echo(all_partitions)

    count_all = 0
    click.echo("calculation_id, uniq_rlzs, uniq_locs, uniq_imts, uniq_gmms, uniq_srcs, uniq_vs30, consistent")
    click.echo("============================================================================================")
    for calc_id in sorted(calc_ids):
        tid_count = 0
        tid_meta = dict(uniq_locs=set(), uniq_imts=set(), uniq_gmms=0, uniq_srcs=0, uniq_vs30s=0)
        sources = set([])
        gmms = set([])

        for partition in all_partitions:
            result = mRLZ.query(
                partition.resample(0.1).code,
                mRLZ.sort_key >= ' ', # partition.resample(0.1).code[:3],
                filter_condition=(mRLZ.hazard_solution_id == calc_id) & (mRLZ.nloc_1 == partition.resample(0.1).code),
            )
            # print(partition.resample(1).code)
            for res in result:
                assert len(res.values) == 27
                imt_count = len(res.values)
                tid_count += imt_count
                count_all += imt_count
                tid_meta['uniq_locs'].add(res.nloc_001)
                tid_meta['uniq_imts'].update(set([v.imt for v in res.values]))
                gmms.add(res.rlz)

        tid_meta['uniq_gmms'] += len(gmms)
        click.echo(
            f"{calc_id}, {tid_count}, {len(tid_meta['uniq_locs']) }, {len(tid_meta['uniq_imts'])}, {tid_meta['uniq_gmms']}, "
            f" - ,  - , - "
        )

        # click.echo(
        #     f"{calc_id}, {df0.shape[0]}, {uniq_locs}, {uniq_imts}, {uniq_gmms}, {uniq_srcs}, {uniq_vs30}, {consistent}"
        # )
        # count_all += df0.shape[0]

        # if bail_on_error and not consistent:
        #     return

    click.echo()
    click.echo(f"Grand total: {count_all}")
    return

#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|

@click.group()
@click.pass_context
def main(context):
    """Import NSHM Model hazard curves to new revision 4 models."""

    context.ensure_object(dict)
    # context.obj['work_folder'] = work_folder


@main.command()
@click.option(
    '--source',
    '-S',
    type=click.Choice(['AWS', 'LOCAL', 'ARROW'], case_sensitive=False),
    default='LOCAL',
    help="set the source store. defaults to LOCAL",
)
@click.option(
    '--ds-name',
    '-D',
    type=str,
    default='pq-CDC',
    help="if dataset is used, then arrow/parquet is queried rather than sqliteas the source store",
)
@click.option(
    '--report',
    '-R',
    type=click.Choice(['LOC', 'ALL'], case_sensitive=False),
    default='LOC',
)
@click.option('-x', '--strict', is_flag=True, default=False, help="abort if consistency checks fail")
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def count_rlz(context, source, ds_name, report, strict, verbose, dry_run):
    """Count the realisations from SOURCE by calculation id

    where LOCAL means local sqlite (v3)\n
        and AWS means AWS (v3)\n
        and ARROW means local arrow (v4)
    """
    if verbose:
        click.echo(f"NZ 0.1grid has {len(nz1_grid)} locations")
        click.echo(f"All (0.1 grid + SRWG + NZ) has {len(all_locs)} locations")
        click.echo(f"All (0.1 grid + SRWG) has {len(nz1_grid + srwg_locs)} locations")

    location = CodedLocation(lat=-39, lon=175.93, resolution=0.001)

    if (source == 'ARROW') and ds_name:
        if report == 'LOC':
            report_arrow_count_loc_rlzs(ds_name, location, verbose)
        elif report == 'ALL':
            report_rlzs_grouped_by_calc(ds_name, verbose, bail_on_error=strict)
        return

    if source == 'AWS':
        #### MONKEYPATCH ...
        toshi_hazard_store.config.REGION = "ap-southeast-2"
        toshi_hazard_store.config.DEPLOYMENT_STAGE = "PROD"
        toshi_hazard_store.config.USE_SQLITE_ADAPTER = False
        # importlib.reload(toshi_hazard_store.model.location_indexed_model)
        importlib.reload(toshi_hazard_store.model.openquake_models)

        # OK this works for reset...
        set_base_class(toshi_hazard_store.model.location_indexed_model.__dict__, 'LocationIndexedModel', Model)
        set_base_class(
            toshi_hazard_store.model.openquake_models.__dict__,
            'OpenquakeRealization',
            toshi_hazard_store.model.location_indexed_model.__dict__['LocationIndexedModel'],
        )

    if source in ['AWS', 'LOCAL']:
        if report == 'LOC':
            report_v3_count_loc_rlzs(location, verbose)
        elif report == 'ALL':
            report_v3_grouped_by_calc(verbose, bail_on_error=strict)


@main.command()
@click.argument('count', type=int)
@click.pass_context
def random_rlz_new(context, count):
    """randomly select realisations loc, hazard_id, rlx and compare the results

    This time the comparison is local THS V3 and local arrow v4
    """

    gtfile = pathlib.Path(__file__).parent / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
    gt_info = json.load(open(str(gtfile)))

    random_args_list = list(get_random_args(gt_info, count))
    set_one = get_table_rows(random_args_list)
    click.echo(set_one)

    def get_arrow_rlzs(random_args_list):
        """This could be faster if locs were grouped into 1 degree bins"""

        result = {}
        for args in random_args_list:
            for loc in args['locs']:
                """
                hazard_query.get_rlz_curves_v3(
                        locs=[loc.code for loc in args['locs']], vs30s=[275], rlzs=[args['rlz']], tids=[args['tid']], imts=['PGA']
                    ):
                """

                dataset = ds.dataset(f'./WORKING/ARROW/pq-CDC/nloc_0={loc.resample(1).code}', format='arrow')
                flt = (pc.field('imt') == pc.scalar("PGA")) & (pc.field("nloc_001") == pc.scalar(loc.code))

                df = dataset.to_table(filter=flt).to_pandas()

            for res in query_table(args):
                obj = res.to_simple_dict(force=True)
                result[obj["sort_key"]] = obj
        return result


@main.command()
@click.argument('count', type=int)
@click.pass_context
def random_rlz_og(context, count):
    """randomly select realisations loc, hazard_id, rlx and compare the results"""

    gtfile = pathlib.Path(__file__).parent / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
    gt_info = json.load(open(str(gtfile)))

    random_args_list = list(get_random_args(gt_info, count))

    set_one = get_table_rows(random_args_list)

    #### MONKEYPATCH ...
    toshi_hazard_store.config.REGION = "ap-southeast-2"
    toshi_hazard_store.config.DEPLOYMENT_STAGE = "PROD"
    toshi_hazard_store.config.USE_SQLITE_ADAPTER = False
    # importlib.reload(toshi_hazard_store.model.location_indexed_model)
    importlib.reload(toshi_hazard_store.model.openquake_models)

    # OK this works for reset...
    set_base_class(toshi_hazard_store.model.location_indexed_model.__dict__, 'LocationIndexedModel', Model)
    set_base_class(
        toshi_hazard_store.model.openquake_models.__dict__,
        'OpenquakeRealization',
        toshi_hazard_store.model.location_indexed_model.__dict__['LocationIndexedModel'],
    )

    def report_differences(dict1, dict2, ignore_keys):
        # print(dict1['sort_key'])
        # print(dict1.keys())
        # print(dict2.keys())
        # print(f"missing_in_dict1_but_in_dict2: {dict2.keys() - dict1}")
        # print(f"missing_in_dict2_but_in_dict1: {dict1.keys() - dict2}")
        diff_cnt = 0
        for key in dict1.keys():
            if key in ignore_keys:
                continue
            if dict1[key] == dict2[key]:
                continue

            print(f"key {key} differs")
            print(dict1[key], dict2[key])
            diff_cnt += 1

        if diff_cnt:
            return 1
        return 0

    set_two = get_table_rows(random_args_list)

    assert len(set_one) == len(set_two)
    ignore_keys = ['uniq_id', 'created', 'source_ids', 'source_tags']
    diff_count = 0
    for key, obj in set_one.items():
        if not obj == set_two[key]:
            diff_count += report_differences(obj, set_two[key], ignore_keys)

    click.echo(f"compared {len(set_one)} realisations with {diff_count} material differences")


if __name__ == "__main__":
    main()
