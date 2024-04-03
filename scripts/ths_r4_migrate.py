# flake8: noqa

"""Console script for preparing to load NSHM hazard curves to new REV4 tables using General Task(s) and nzshm-model.

This is NSHM process specific, as it assumes the following:
 - hazard producer metadata is available from the NSHM toshi-api via **nshm-toshi-client** library
 - NSHM model characteristics are available in the **nzshm-model** library

Hazard curves are store using the new THS Rev4 tables which may also be used independently.

Given a general task containing hazard calcs used in NHSM, we want to iterate over the sub-tasks and do
the setup required for importing the hazard curves:

    - pull the configs and check we have a compatible producer config (or ...) cmd `producers`
    - optionally create new producer configs automatically, and record info about these
       - NB if new producer configs are created, then it is the users responsibility to assign
         a CompatibleCalculation to each

These things may get a separate script
    - OPTION to download HDF5 and load hazard curves from there
    - OPTION to import V3 hazard curves from DynamodDB and extract ex
"""
import collections
import datetime as dt
import logging
import os
import pathlib
import time
# from typing import Iterable
# from .store_hazard_v3 import extract_and_save
import click

# try:
#     from openquake.calculators.extract import Extractor
# except (ModuleNotFoundError, ImportError):
#     print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
#     raise

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
# logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)
logging.getLogger('nzshm_model').setLevel(logging.INFO)
logging.getLogger('gql.transport').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store.db_adapter.sqlite.sqlite_store').setLevel(logging.WARNING)


# import toshi_hazard_store  # noqa: E402
from toshi_hazard_store.config import DEPLOYMENT_STAGE as THS_STAGE
from toshi_hazard_store.config import LOCAL_CACHE_FOLDER
from toshi_hazard_store.config import REGION as THS_REGION
from toshi_hazard_store.config import USE_SQLITE_ADAPTER
from toshi_hazard_store.oq_import import (  # noqa: E402
    create_producer_config,
    # export_rlzs_rev4,
    get_compatible_calc,
    get_producer_config,
)
from toshi_hazard_store.multi_batch import save_parallel

# from toshi_hazard_store import model
from toshi_hazard_store.model.revision_4 import hazard_models

from .revision_4 import aws_ecr_docker_image as aws_ecr
from .revision_4 import oq_config

from toshi_hazard_store.oq_import.oq_manipulate_hdf5 import migrate_nshm_uncertainty_string
from toshi_hazard_store.oq_import.parse_oq_realizations import rlz_mapper_from_dataframes

import pandas
# from toshi_hazard_store.query import hazard_query
# from toshi_hazard_store.model import OpenquakeRealization
import toshi_hazard_store.model

from nzshm_common.grids import load_grid
from nzshm_common.location.code_location import CodedLocation
# from nzshm_common.location.location import LOCATIONS_BY_ID
from nzshm_common import location
# import json

ECR_REGISTRY_ID = '461564345538.dkr.ecr.us-east-1.amazonaws.com'
ECR_REPONAME = "nzshm22/runzi-openquake"

from nzshm_model.logic_tree.source_logic_tree.toshi_api import (  # noqa: E402 and this function be in the client !
    get_secret,
)

from .revision_4 import toshi_api_client  # noqa: E402

# Get API key from AWS secrets manager
API_URL = os.getenv('NZSHM22_TOSHI_API_URL', "http://127.0.0.1:5000/graphql")
try:
    if 'TEST' in API_URL.upper():
        API_KEY = get_secret("NZSHM22_TOSHI_API_SECRET_TEST", "us-east-1").get("NZSHM22_TOSHI_API_KEY_TEST")
    elif 'PROD' in API_URL.upper():
        API_KEY = get_secret("NZSHM22_TOSHI_API_SECRET_PROD", "us-east-1").get("NZSHM22_TOSHI_API_KEY_PROD")
    else:
        API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY', "")
    # print(f"key: {API_KEY}")
except AttributeError as err:
    print(f"unable to get secret from secretmanager: {err}")
    API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY', "")
S3_URL = None
DEPLOYMENT_STAGE = os.getenv('DEPLOYMENT_STAGE', 'LOCAL').upper()
REGION = os.getenv('REGION', 'ap-southeast-2')  # SYDNEY

def echo_settings(work_folder, verbose=True):
    click.echo('\nfrom command line:')
    click.echo(f"   using verbose: {verbose}")
    click.echo(f"   using work_folder: {work_folder}")

    try:
        click.echo('\nfrom API environment:')
        click.echo(f'   using API_URL: {API_URL}')
        click.echo(f'   using REGION: {REGION}')
        click.echo(f'   using DEPLOYMENT_STAGE: {DEPLOYMENT_STAGE}')
    except Exception:
        pass

    click.echo('\nfrom THS config:')
    click.echo(f'   using LOCAL_CACHE_FOLDER: {LOCAL_CACHE_FOLDER}')
    click.echo(f'   using THS_STAGE: {THS_STAGE}')
    click.echo(f'   using THS_REGION: {THS_REGION}')
    click.echo(f'   using USE_SQLITE_ADAPTER: {USE_SQLITE_ADAPTER}')

def migrate_realisations_from_subtask(
    subtask_info: 'SubtaskRecord', partition, compatible_calc, verbose, update, dry_run=False
):

    """Migrate all the realisations for the given subtask

        # Get the gsim_lt from the relevant meta record
        # mofify the gsim_lt
        # obtain the source and hash_keys

        # query the source table
        # for res:
        #   write to the target
    """

    if verbose:
        click.echo(subtask_info)

    producer_software = f"{ECR_REGISTRY_ID}/{ECR_REPONAME}"
    producer_version_id = subtask_info.image['imageDigest'][7:27]  # first 20 bits of hashdigest
    configuration_hash = subtask_info.config_hash
    pc_key = (partition, f"{producer_software}:{producer_version_id}:{configuration_hash}")

    # check for existing
    producer_config = get_producer_config(pc_key, compatible_calc)
    if producer_config:
        if verbose:
            click.echo(f'found producer_config {pc_key} ')
        # if update:
        #     producer_config.notes = "notes 2"
        #     producer_config.save()
        #     click.echo(f'updated producer_config {pc_key} ')

    if producer_config is None:
        producer_config = create_producer_config(
            partition_key=partition,
            compatible_calc=compatible_calc,
            extractor=None,
            tags=subtask_info.image['imageTags'],
            effective_from=subtask_info.image['imagePushedAt'],
            last_used=subtask_info.image['lastRecordedPullTime'],
            producer_software=producer_software,
            producer_version_id=producer_version_id,
            configuration_hash=configuration_hash,
            # configuration_data=config.config_hash,
            notes="notes",
            dry_run=dry_run,
        )
        if verbose:
            click.echo(
                f"New Model {producer_config} has foreign key ({producer_config.partition_key}, {producer_config.range_key})"
            )


    mMeta   = toshi_hazard_store.model.openquake_models.ToshiOpenquakeMeta
    mRLZ_V4 = hazard_models.HazardRealizationCurve
    mRLZ_V3 = toshi_hazard_store.model.openquake_models.OpenquakeRealization

    #Get the V3 Metadata ...
    query = mMeta.query(
        "ToshiOpenquakeMeta",
        mMeta.hazsol_vs30_rk==f"{subtask_info.hazard_calc_id}:{subtask_info.vs30}"
    )

    try:
        meta = next(query)
    except StopIteration:
        log.warning(f"Metadata for {subtask_info.hazard_calc_id}:{subtask_info.vs30} was not found. Terminating migration.")
        return

    gsim_lt = pandas.read_json(meta.gsim_lt)
    source_lt = pandas.read_json(meta.src_lt)
    rlz_lt = pandas.read_json(meta.rlz_lt)

    #apply gsim migrations
    gsim_lt["uncertainty"] = gsim_lt["uncertainty"].map(migrate_nshm_uncertainty_string)

    # build the realisation mapper
    rlz_map = rlz_mapper_from_dataframes(source_lt=source_lt, gsim_lt=gsim_lt, rlz_lt=rlz_lt)

    t3 = time.perf_counter()
    grid = load_grid('NZ_0_1_NB_1_1')

    count = 0
    for location in [CodedLocation(o[0], o[1], 0.1) for o in grid]:
        for source_rlz in mRLZ_V3.query(
            location.code,
            mRLZ_V3.sort_key >= location.resample(0.001).code,
            filter_condition=(mRLZ_V3.hazard_solution_id == subtask_info.hazard_calc_id) & (mRLZ_V3.vs30 == subtask_info.vs30)
            ):
            count += 1
            # print(source_rlz.partition_key, source_rlz.vs30, source_rlz.rlz)
            # print(rlz_map[source_rlz.rlz].gmms.hash_digest, source_rlz.values[-1].imt)
            realization = rlz_map[source_rlz.rlz]
            for imt_values in source_rlz.values:
                log.debug(realization)
                target_realization = mRLZ_V4(
                    compatible_calc_fk=compatible_calc.foreign_key(),
                    producer_config_fk=producer_config.foreign_key(),
                    created = source_rlz.created,
                    calculation_id=subtask_info.hazard_calc_id,
                    values=list(imt_values.vals),
                    imt=imt_values.imt,
                    vs30=source_rlz.vs30,
                    site_vs30=source_rlz.site_vs30,
                    source_digests=[realization.sources.hash_digest],
                    gmm_digests=[realization.gmms.hash_digest],
                )
                yield target_realization.set_location(CodedLocation(lat=source_rlz.lat, lon=source_rlz.lon, resolution=0.001))

SubtaskRecord = collections.namedtuple(
    'SubtaskRecord', 'gt_id, hazard_calc_id, config_hash, image, vs30'
)

def process_gt_subtasks(gt_id: str, work_folder:str, verbose:bool = False):
    subtasks_folder = pathlib.Path(work_folder, gt_id, 'subtasks')
    subtasks_folder.mkdir(parents=True, exist_ok=True)

    if verbose:
        click.echo('fetching ECR stash')
    ecr_repo_stash = aws_ecr.ECRRepoStash(
        ECR_REPONAME, oldest_image_date=dt.datetime(2023, 3, 20, tzinfo=dt.timezone.utc)
    ).fetch()

    headers = {"x-api-key": API_KEY}
    gtapi = toshi_api_client.ApiClient(API_URL, None, with_schema_validation=False, headers=headers)

    def get_hazard_task_ids(query_res):
        for edge in query_res['children']['edges']:
            yield edge['node']['child']['id']

    query_res = gtapi.get_gt_subtasks(gt_id)

    for task_id in get_hazard_task_ids(query_res):

        # completed already
        if task_id in ['T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3', 'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3',
            'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE4', 'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI0',
            "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI2",
            "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDMy", "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI5"]:
            continue

        query_res = gtapi.get_oq_hazard_task(task_id)
        log.debug(query_res)
        task_created = dt.datetime.fromisoformat(query_res["created"])  # "2023-03-20T09:02:35.314495+00:00",
        log.debug(f"task created: {task_created}")

        oq_config.download_artefacts(gtapi, task_id, query_res, subtasks_folder)
        jobconf = oq_config.config_from_task(task_id, subtasks_folder)

        config_hash = jobconf.compatible_hash_digest()
        latest_engine_image = ecr_repo_stash.active_image_asat(task_created)
        log.debug(latest_engine_image)

        log.debug(f"task {task_id} hash: {config_hash}")

        yield SubtaskRecord(
            gt_id=gt_id,
            hazard_calc_id=task_id,
            image=latest_engine_image,
            config_hash=config_hash,
            vs30=jobconf.config.get('site_params', 'reference_vs30_value'),
        )


#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|

@click.group()
@click.option('--work_folder', '-W', default=lambda: os.getcwd(), help="defaults to Current Working Directory")
@click.pass_context
def main(context, work_folder):
    """Import NSHM Model hazard curves to new revision 4 models."""

    context.ensure_object(dict)
    context.obj['work_folder'] = work_folder

@main.command()
@click.argument('gt_id')
@click.argument('partition')
@click.argument('compat_calc')
@click.option(
    '--update',
    '-U',
    is_flag=True,
    default=False,
    help="overwrite existing producer record (versioned table).",
)
@click.option(
    '--source',
    '-S',
    type=click.Choice(['AWS', 'LOCAL'], case_sensitive=False),
    default='LOCAL',
)
@click.option(
    '--target',
    '-S',
    type=click.Choice(['AWS', 'LOCAL'], case_sensitive=False),
    default='LOCAL',
)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def migrate(
    context,
    gt_id,
    partition,
    compat_calc,
    update,
    source,
    target,
    verbose,
    dry_run,
):
    """Migrate realisations from V3 to R4 table for GT_ID PARTITION and COMPAT_CALC

    GT_ID is an NSHM General task id containing HazardAutomation Tasks\n
    PARTITION is a table partition (hash) for Producer\n
    COMPAT is foreign key of the compatible_calc in form `A_B`

    Notes:\n
    - pull the configs and check we have a compatible producer config\n
    - optionally, create any new producer configs
    """
    work_folder = context.obj['work_folder']

    compatible_calc = get_compatible_calc(compat_calc.split("_"))
    if compatible_calc is None:
        raise ValueError(f'compatible_calc: {compat_calc} was not found')

    if verbose:
        click.echo('fetching General Task subtasks')

    # def get_hazard_task_ids(query_res):
    #     for edge in query_res['children']['edges']:
    #         yield edge['node']['child']['id']

    # # configure the input/output tables for proper source/target setup
    # # let's default to local table to get this running...
    # query_res = gtapi.get_gt_subtasks(gt_id)
    def generate_models():
        for subtask_info in process_gt_subtasks(gt_id, work_folder=work_folder, verbose=verbose):
            log.info(f"Processing subtask {subtask_info.hazard_calc_id} in gt {gt_id}")
            count = 0
            for new_rlz in migrate_realisations_from_subtask(subtask_info, partition, compatible_calc, verbose, update, dry_run=False):
                count += 1
                yield new_rlz

            log.info(f"Produced {count} source objects from {subtask_info.hazard_calc_id} in {gt_id}")


    if dry_run:
        for itm in generate_models():
            pass
        log.info("Dry run completed")
    else:
        save_parallel("", generate_models(), hazard_models.HazardRealizationCurve, 1, 100)

if __name__ == "__main__":
    main()
