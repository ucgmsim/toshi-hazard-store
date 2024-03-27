"""Console script for preparing to load NSHM hazard curves to new REV4 tables using General Task(s) and nzshm-model.

This is NSHM process specific, as it assumes the following:
 - hazard producer metadata is available from the NSHM toshi-api via **nshm-toshi-client** library
 - NSHM model characteristics are available in the **nzshm-model** library

Hazard curves are store using the new THS Rev4 tables which may also be used independently.

Given a general task containing hazard calcs used in NHSM, we want to iterate over the sub-tasks and do the setup required
for importing the hazard curves:

    - pull the configs and check we have a compatible producer config (or ...) cmd `producers`
    - optionally create new producer configs automatically, and record info about these
       - NB if new producer configs are created, then it is the users responsibility to assign a CompatibleCalculation to each

These things may get a separate script
    - OPTION to download HDF5 and load hazard curves from there
    - OPTION to import V3 hazard curves from DynamodDB and extract ex
"""

import datetime as dt
import logging
import os
import pathlib
import click
import requests
import zipfile
import collections

from typing import Iterable

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)
logging.getLogger('nzshm_model').setLevel(logging.DEBUG)
logging.getLogger('gql.transport').setLevel(logging.WARNING)

try:
    from openquake.calculators.extract import Extractor
except (ModuleNotFoundError, ImportError):
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
    raise

import nzshm_model  # noqa: E402
import toshi_hazard_store  # noqa: E402
from toshi_hazard_store.model.revision_4 import hazard_models  # noqa: E402
from toshi_hazard_store.oq_import import (  # noqa: E402
    create_producer_config,
    export_rlzs_rev4,
    get_compatible_calc,
    get_producer_config,
)
from .revision_4 import oq_config, aws_ecr_docker_image as aws_ecr

from toshi_hazard_store.config import (
    USE_SQLITE_ADAPTER,
    LOCAL_CACHE_FOLDER,
    DEPLOYMENT_STAGE as THS_STAGE,
    REGION as THS_REGION,
)

ECR_REGISTRY_ID = '461564345538.dkr.ecr.us-east-1.amazonaws.com'
ECR_REPONAME = "nzshm22/runzi-openquake"


from .revision_4 import toshi_api_client

from nzshm_model.logic_tree.source_logic_tree.toshi_api import (
    get_secret,
)  # noqa: E402 and this function be in the client !


# formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
# root_handler = log.handlers[0]
# root_handler.setFormatter(formatter)

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


def get_extractor(calc_id: str):
    """return an extractor for given calc_id or path to hdf5"""
    hdf5_path = pathlib.Path(calc_id)
    try:
        if hdf5_path.exists():
            # we have a file path to work with
            extractor = Extractor(str(hdf5_path))
        else:
            extractor = Extractor(int(calc_id))
    except Exception as err:
        log.info(err)
        return None
    return extractor

def echo_settings(work_folder, verbose=True):
    click.echo('\nfrom command line:')
    click.echo(f"   using verbose: {verbose}")
    click.echo(f"   using work_folder: {work_folder}")

    try:
        click.echo('\nfrom API environment:')
        click.echo(f'   using API_URL: {API_URL}')
        click.echo(f'   using REGION: {REGION}')
        click.echo(f'   using DEPLOYMENT_STAGE: {DEPLOYMENT_STAGE}')
    except:
        pass

    click.echo('\nfrom THS config:')
    click.echo(f'   using LOCAL_CACHE_FOLDER: {LOCAL_CACHE_FOLDER}')
    click.echo(f'   using THS_STAGE: {THS_STAGE}')
    click.echo(f'   using THS_REGION: {THS_REGION}')
    click.echo(f'   using USE_SQLITE_ADAPTER: {USE_SQLITE_ADAPTER}')


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
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def create_tables(context, verbose, dry_run):

    work_folder = context.obj['work_folder']
    if verbose:
        echo_settings(work_folder)
    if dry_run:
        click.echo('SKIP: Ensuring tables exist.')
    else:
        click.echo('Ensuring tables exist.')
        toshi_hazard_store.model.migrate_r4()



@main.command()
@click.argument('gt_list', type=click.File('rb'))
@click.argument('partition')
@click.option(
    '--compatible_calc_fk',
    '-CCF',
    default="A_A",
    required=True,
    help="foreign key of the compatible_calc in form `A_B`",
)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def prod_from_gtfile(
    context,
    gt_list,
    partition,
    compatible_calc_fk,
    # update,
    # software, version, hashed, config, notes,
    verbose,
    dry_run,
):
    """Prepare and validate Producer Configs a given file of GT_IDa in a PARTITION"""
    for gt_id in gt_list:
        click.echo(F"call producers for {gt_id.decode().strip()}")
        # continue
        context.invoke(producers,
            gt_id=gt_id.decode().strip(),
            partition=partition,
            compatible_calc_fk=compatible_calc_fk,
            update = False,
            # software, version, hashed, config, notes,
            verbose=verbose,
            dry_run=dry_run
        )
    click.echo("ALL DONE")


@main.command()
@click.argument('gt_id')
@click.argument('partition')
@click.option(
    '--compatible_calc_fk',
    '-CCF',
    default="A_A",
    required=True,
    help="foreign key of the compatible_calc in form `A_B`",
)
@click.option(
    '--update',
    '-U',
    is_flag=True,
    default=False,
    help="overwrite existing producer record (versioned table).",
)
@click.option(
    '--with_rlzs',
    '-R',
    is_flag=True,
    default=False,
    help="also get the realisations",
)
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def producers(
    context,
    # model_id,
    gt_id,
    partition,
    compatible_calc_fk,
    update,
    with_rlzs,
    # software, version, hashed, config, notes,
    verbose,
    dry_run,
):
    """Prepare and validate Producer Configs for a given GT_ID in a PARTITION

    GT_ID is an NSHM General task id containing HazardAutomation Tasks\n
    PARTITION is a table partition (hash)

    Notes:\n
    - pull the configs and check we have a compatible producer config\n
    - optionally, create any new producer configs
    """

    work_folder = context.obj['work_folder']

    headers = {"x-api-key": API_KEY}
    gtapi = toshi_api_client.ApiClient(API_URL, None, with_schema_validation=False, headers=headers)

    if verbose:
        echo_settings(work_folder)

    compatible_calc = get_compatible_calc(compatible_calc_fk.split("_"))
    if compatible_calc is None:
        raise ValueError(f'compatible_calc: {compatible_calc_fk} was not found')


    if verbose:
        click.echo('fetching ECR stash')
    ecr_repo_stash = aws_ecr.ECRRepoStash(
        ECR_REPONAME, oldest_image_date=dt.datetime(2023, 3, 20, tzinfo=dt.timezone.utc)
    ).fetch()

    if verbose:
        click.echo('fetching General Task subtasks')
    query_res = gtapi.get_gt_subtasks(gt_id)

    SubtaskRecord = collections.namedtuple('SubtaskRecord', 'hazard_calc_id, config_hash, image, hdf5_path')
    def handle_subtasks(gt_id: str, subtask_ids: Iterable):
        subtasks_folder = pathlib.Path(work_folder, gt_id, 'subtasks')
        subtasks_folder.mkdir(parents=True, exist_ok=True)

        for task_id in subtask_ids:
            query_res = gtapi.get_oq_hazard_task(task_id)
            log.debug(query_res)
            task_created = dt.datetime.fromisoformat(query_res["created"])  # "2023-03-20T09:02:35.314495+00:00",
            log.debug(f"task created: {task_created}")

            oq_config.download_artefacts(gtapi, task_id, query_res, subtasks_folder, include_hdf5=with_rlzs)
            jobconf = oq_config.config_from_task(task_id, subtasks_folder)

            config_hash = jobconf.compatible_hash_digest()
            latest_engine_image = ecr_repo_stash.active_image_asat(task_created)
            log.debug(latest_engine_image)

            log.debug(f"task {task_id} hash: {config_hash}")

            if with_rlzs:
                hdf5_path = oq_config.hdf5_from_task(task_id, subtasks_folder)
            else:
                hdf5_path=None
                
            yield SubtaskRecord(
                hazard_calc_id=task_id,
                image=latest_engine_image, 
                config_hash=config_hash,
                hdf5_path=hdf5_path)

    def get_hazard_task_ids(query_res):
        for edge in query_res['children']['edges']:
            yield edge['node']['child']['id']

    extractor=None
    for subtask_info in handle_subtasks(gt_id, get_hazard_task_ids(query_res)):

        if verbose:
            click.echo(subtask_info)
            
        producer_software = f"{ECR_REGISTRY_ID}/{ECR_REPONAME}"
        producer_version_id = subtask_info.image['imageDigest'][7:27] # first 20 bits of hashdigest
        configuration_hash = subtask_info.config_hash
        pc_key = (partition, f"{producer_software}:{producer_version_id}:{configuration_hash}")

        #check for existing
        producer_config = get_producer_config(pc_key, compatible_calc)
        if producer_config:
            if verbose:
                click.echo(f'found producer_config {pc_key} ')
            if update:
                producer_config.notes = "notes 2"
                producer_config.save()
                click.echo(f'updated producer_config {pc_key} ')
        if producer_config is None:
            model = create_producer_config(
                partition_key=partition,
                compatible_calc=compatible_calc,
                extractor=extractor,
                tags = subtask_info.image['imageTags'],
                effective_from = subtask_info.image['imagePushedAt'],
                last_used = subtask_info.image['lastRecordedPullTime'],
                producer_software=producer_software,
                producer_version_id=producer_version_id,
                configuration_hash=configuration_hash,
                # configuration_data=config.config_hash,
                notes="notes",
                dry_run=dry_run,
            )
            if verbose:
                click.echo(f"New Model {model} has foreign key ({model.partition_key}, {model.range_key})")

        if with_rlzs:
            extractor = Extractor(str(subtask_info.hdf5_path))
            export_rlzs_rev4(
                extractor,
                compatible_calc=compatible_calc,
                producer_config=producer_config,
                hazard_calc_id=subtask_info.hazard_calc_id,
                vs30=400,
                return_rlz=False,
                update_producer=True
            )
            assert 0

if __name__ == "__main__":
    main()
