"""
Console script for querying tables before and after import/migration to ensure that we have what we expect
"""
import importlib
import logging
import click
import pathlib
import json
log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)

import toshi_hazard_store  # noqa: E402

from scripts.core import echo_settings

import toshi_hazard_store.model.revision_4.hazard_models  # noqa: E402
import toshi_hazard_store.model.openquake_models
import toshi_hazard_store.config
import toshi_hazard_store.query.hazard_query

from nzshm_common.grids import load_grid

from toshi_hazard_store.config import (
    USE_SQLITE_ADAPTER,
    LOCAL_CACHE_FOLDER,
    DEPLOYMENT_STAGE as THS_STAGE,
    REGION as THS_REGION,
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
    help="set the source store. defaults to LOCAL"
)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def count_rlz(context, source, verbose, dry_run):
    """Count the items in the table in SOURCE"""

    click.echo(f"NZ 0.1grid has {len(nz1_grid)} locations")

    if source == "OLD-LOCAL":
        click.echo()
        click.echo("count() not supported by adapter: please use `sqlite3> select count(*) from THS_OpenquakeRealization;` instead")
        return
    else:
        # count_rlzs(locations, tids, rlzs)

        # mRLZ = toshi_hazard_store.model.openquake_models.OpenquakeRealization

        # print(mRLZ.Meta.region)
        # toshi_hazard_store.config.REGION = "ap-southeast-2"
        # toshi_hazard_store.config.DEPLOYMENT_STAGE = "PROD"
        importlib.reload(toshi_hazard_store.model.openquake_models)
        mRLZ = toshi_hazard_store.model.openquake_models.OpenquakeRealization

        gtfile = pathlib.Path(__file__).parent.parent.parent / "toshi_hazard_store" / "query" / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
        gt_info = json.load(open(str(gtfile)))
        tids = [edge['node']['child']['hazard_solution']["id"] for edge in gt_info['data']['node']['children']['edges']]

        click.echo(tids)
        click.echo()
        count_all = 0
        for tid in tids:
            rlz_count = mRLZ.count(
                "-42.4~171.2",
                mRLZ.sort_key >= f'-42.450~171.210:275:000000:{tid}',
                filter_condition=(mRLZ.nloc_001 == "-42.450~171.210") & (mRLZ.hazard_solution_id == tid)
                )
            count_all += rlz_count
            click.echo(f"-42.450~171.210, {tid}, {rlz_count}")

        click.echo()
        click.echo(f"Grand total: {count_all}")

@main.command()
@click.option(
    '--source',
    '-S',
    type=click.Choice(['AWS', 'LOCAL'], case_sensitive=False),
    default='LOCAL',
    help="set the source store. defaults to LOCAL"
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

    gtfile = pathlib.Path(__file__).parent.parent.parent / "toshi_hazard_store" / "query" / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
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
            filter_condition=(mRLZ.nloc_001 == "-42.450~171.210") & (mRLZ.hazard_solution_id == tid)
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
