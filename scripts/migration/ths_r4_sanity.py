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

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
# logging.getLogger('pynamodb').setLevel(logging.DEBUG)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('toshi_hazard_store').setLevel(logging.WARNING)

from nzshm_common.grids import load_grid
from nzshm_common.location.code_location import CodedLocation
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
    type=click.Choice(['AWS', 'LOCAL'], case_sensitive=False),
    default='LOCAL',
    help="set the source store. defaults to LOCAL",
)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def count_rlz(context, source, verbose, dry_run):
    """Count the items in the table in SOURCE"""

    click.echo(f"NZ 0.1grid has {len(nz1_grid)} locations")

    if source == "OLD-LOCAL":
        click.echo()
        click.echo(
            "count() not supported by adapter: please use `sqlite3> select count(*) from THS_OpenquakeRealization;` instead"
        )
        return
    else:
        # count_rlzs(locations, tids, rlzs)

        # mRLZ = toshi_hazard_store.model.openquake_models.OpenquakeRealization

        # print(mRLZ.Meta.region)

        #### MONKEYPATCH ...
        # toshi_hazard_store.config.REGION = "ap-southeast-2"
        # toshi_hazard_store.config.DEPLOYMENT_STAGE = "PROD"
        # importlib.reload(toshi_hazard_store.model.openquake_models)
        ####
        mRLZ = toshi_hazard_store.model.openquake_models.OpenquakeRealization

        gtfile = (
            pathlib.Path(__file__).parent.parent.parent
            / "toshi_hazard_store"
            / "query"
            / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
        )
        gt_info = json.load(open(str(gtfile)))
        tids = [edge['node']['child']['hazard_solution']["id"] for edge in gt_info['data']['node']['children']['edges']]

        click.echo(tids)
        click.echo()
        count_all = 0
        for tid in tids:
            rlz_count = mRLZ.count(
                "-42.4~171.2",
                mRLZ.sort_key >= f'-42.450~171.210:275:000000:{tid}',
                filter_condition=(mRLZ.nloc_001 == "-42.450~171.210") & (mRLZ.hazard_solution_id == tid),
            )
            count_all += rlz_count
            click.echo(f"-42.450~171.210, {tid}, {rlz_count}")

        click.echo()
        click.echo(f"Grand total: {count_all}")


@main.command()
@click.argument('count', type=int)
@click.pass_context
def random_rlz(context, count):
    """randomly select realisations loc, hazard_id, rlx and compare the results"""

    gtfile = pathlib.Path(__file__).parent / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
    gt_info = json.load(open(str(gtfile)))

    def get_random_args(how_many):
        for n in range(how_many):
            yield dict(
                tid=random.choice(
                    [
                        edge['node']['child']["hazard_solution"]["id"]
                        for edge in gt_info['data']['node']['children']['edges']
                    ]
                ),
                rlz=random.choice(range(20)),
                locs=[CodedLocation(o[0], o[1], 0.001).code for o in random.sample(nz1_grid, how_many)],
            )

    def query_table(args):
        # mRLZ = toshi_hazard_store.model.openquake_models.__dict__['OpenquakeRealization']
        importlib.reload(toshi_hazard_store.query.hazard_query)
        for res in toshi_hazard_store.query.hazard_query.get_rlz_curves_v3(
            locs=args['locs'], vs30s=[275], rlzs=[args['rlz']], tids=[args['tid']], imts=['PGA']
        ):
            yield (res)

    def get_table_rows(random_args_list):
        result = {}
        for args in random_args_list:
            for res in query_table(args):
                obj = res.to_simple_dict(force=True)
                result[obj["sort_key"]] = obj
        return result

    random_args_list = list(get_random_args(count))

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


@main.command()
@click.option(
    '--source',
    '-S',
    type=click.Choice(['AWS', 'LOCAL'], case_sensitive=False),
    default='LOCAL',
    help="set the source store. defaults to LOCAL",
)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def find_extra_rlz(context, source, verbose, dry_run):
    """Count the items in the table in SOURCE"""

    click.echo(f"NZ 0.1grid has {len(nz1_grid)} locations")

    # toshi_hazard_store.config.REGION = "ap-southeast-2"
    # toshi_hazard_store.config.DEPLOYMENT_STAGE = "PROD"
    importlib.reload(toshi_hazard_store.model.openquake_models)
    mRLZ = toshi_hazard_store.model.openquake_models.OpenquakeRealization

    gtfile = (
        pathlib.Path(__file__).parent.parent.parent
        / "toshi_hazard_store"
        / "query"
        / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
    )
    gt_info = json.load(open(str(gtfile)))
    tids = [edge['node']['child']["id"] for edge in gt_info['data']['node']['children']['edges']]

    # check to hazard_sol outside what we expect .. (Maybe some trawsh left over ???)
    click.echo(tids)
    click.echo()
    count_all = 0
    for tid in tids:
        rlz_count = mRLZ.count(
            "-42.4~171.2",
            mRLZ.sort_key >= f'-42.450~171.210:275:000000:{tid}',
            filter_condition=(mRLZ.nloc_001 == "-42.450~171.210") & (mRLZ.hazard_solution_id == tid),
        )
        count_all += rlz_count
        click.echo(f"-42.450~171.210, {tid}, {rlz_count}")

    click.echo()
    click.echo(f"Grand total: {count_all}")

    locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in nz1_grid]
    # # check count by loc dimension
    # click.echo(tids)
    # click.echo()
    # count_all = 0
    # for loc in locs:
    #     rlz_count = mRLZ.count(
    #         loc.resample(0,1).code,
    #         mRLZ.sort_key >= f'{loc.code}:275',
    #         filter_condition=(mRLZ.nloc_001 == loc.code) & (mRLZ.hazard_solution_id.is_in(*tids)
    #         )
    #     count_all += rlz_count
    #     click.echo(f"{loc.code}, {rlz_count}")

    # click.echo()
    # click.echo(f"Grand total: {count_all}")


if __name__ == "__main__":
    main()
