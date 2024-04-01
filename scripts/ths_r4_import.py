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
from typing import Iterable
from .store_hazard_v3 import extract_and_save
import click

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)
logging.getLogger('nzshm_model').setLevel(logging.INFO)
logging.getLogger('gql.transport').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.INFO)

try:
    from openquake.calculators.extract import Extractor
except (ModuleNotFoundError, ImportError):
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
    raise

# import nzshm_model  # noqa: E402

import toshi_hazard_store  # noqa: E402
from toshi_hazard_store.config import DEPLOYMENT_STAGE as THS_STAGE
from toshi_hazard_store.config import LOCAL_CACHE_FOLDER
from toshi_hazard_store.config import REGION as THS_REGION
from toshi_hazard_store.config import USE_SQLITE_ADAPTER
from toshi_hazard_store.oq_import import (  # noqa: E402
    create_producer_config,
    export_rlzs_rev4,
    get_compatible_calc,
    get_producer_config,
)
# from toshi_hazard_store import model
from toshi_hazard_store.model.revision_4 import hazard_models

from .revision_4 import aws_ecr_docker_image as aws_ecr
from .revision_4 import oq_config

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
    except Exception:
        pass

    click.echo('\nfrom THS config:')
    click.echo(f'   using LOCAL_CACHE_FOLDER: {LOCAL_CACHE_FOLDER}')
    click.echo(f'   using THS_STAGE: {THS_STAGE}')
    click.echo(f'   using THS_REGION: {THS_REGION}')
    click.echo(f'   using USE_SQLITE_ADAPTER: {USE_SQLITE_ADAPTER}')


def handle_import_subtask_rev4(subtask_info: 'SubtaskRecord', partition, compatible_calc, verbose, update, with_rlzs, dry_run=False):

    if verbose:
        click.echo(subtask_info)
    
    extractor = None

    producer_software = f"{ECR_REGISTRY_ID}/{ECR_REPONAME}"
    producer_version_id = subtask_info.image['imageDigest'][7:27]  # first 20 bits of hashdigest
    configuration_hash = subtask_info.config_hash
    pc_key = (partition, f"{producer_software}:{producer_version_id}:{configuration_hash}")

    # check for existing
    producer_config = get_producer_config(pc_key, compatible_calc)
    if producer_config:
        if verbose:
            click.echo(f'found producer_config {pc_key} ')
        if update:
            producer_config.notes = "notes 2"
            producer_config.save()
            click.echo(f'updated producer_config {pc_key} ')
    
    if producer_config is None:
        producer_config = create_producer_config(
            partition_key=partition,
            compatible_calc=compatible_calc,
            extractor=extractor,
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
            click.echo(f"New Model {producer_config} has foreign key ({producer_config.partition_key}, {producer_config.range_key})")

    if with_rlzs:
        extractor = Extractor(str(subtask_info.hdf5_path))
        export_rlzs_rev4(
            extractor,
            compatible_calc=compatible_calc,
            producer_config=producer_config,
            hazard_calc_id=subtask_info.hazard_calc_id,
            vs30=subtask_info.vs30,
            return_rlz=False,
            update_producer=True,
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
@click.option(
    '--process_v3',
    '-P3',
    is_flag=True,
    default=False,
    help="V3 instead of v4",
)
@click.pass_context
def create_tables(context, process_v3):

    if process_v3:
        click.echo('Ensuring V3 openquake tables exist.')
        toshi_hazard_store.model.migrate_openquake()
    else:
        click.echo('Ensuring Rev4 tables exist.')
        toshi_hazard_store.model.migrate_r4()


@main.command()
@click.argument('partition')
@click.option('--uniq', '-U', required=False, default=None, help="uniq_id, if not specified a UUID will be used")
@click.option('--notes', '-N', required=False, default=None, help="optional notes about the item")
@click.option(
    '-d',
    '--dry-run',
    is_flag=True,
    default=False,
    help="dont actually do anything.",
)
def compat(partition, uniq, notes, dry_run):
    """create a new hazard calculation compatability identifier in PARTITION"""

    mCHC = hazard_models.CompatibleHazardCalculation

    t0 = dt.datetime.utcnow()
    if uniq:
        m = mCHC(partition_key=partition, uniq_id=uniq, notes=notes)
    else:
        m = mCHC(partition_key=partition, notes=notes)

    if not dry_run:
        m.save()
        t1 = dt.datetime.utcnow()
        click.echo("Done saving CompatibleHazardCalculation, took %s secs" % (t1 - t0).total_seconds())
    else:
        click.echo('SKIP: saving CompatibleHazardCalculation.')


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
        context.invoke(
            producers,
            gt_id=gt_id.decode().strip(),
            partition=partition,
            compatible_calc_fk=compatible_calc_fk,
            update=False,
            # software, version, hashed, config, notes,
            verbose=verbose,
            dry_run=dry_run,
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
@click.option(
    '--process_v3',
    '-P3',
    is_flag=True,
    default=False,
    help="V3 instead of v4",
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
    process_v3,
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

    if verbose:
        click.echo('fetching ECR stash')
    ecr_repo_stash = aws_ecr.ECRRepoStash(
        ECR_REPONAME, oldest_image_date=dt.datetime(2023, 3, 20, tzinfo=dt.timezone.utc)
    ).fetch()

    if verbose:
        click.echo('fetching General Task subtasks')
    query_res = gtapi.get_gt_subtasks(gt_id)

    SubtaskRecord = collections.namedtuple('SubtaskRecord', 'gt_id, hazard_calc_id, config_hash, image, hdf5_path, vs30')

    def handle_subtasks(gt_id: str, subtask_ids: Iterable):
        subtasks_folder = pathlib.Path(work_folder, gt_id, 'subtasks')
        subtasks_folder.mkdir(parents=True, exist_ok=True)

        for task_id in subtask_ids:

            # completed already
            if task_id in ['T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3', 'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3']:
                continue

            # problems
            if task_id in ['T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE4', 'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI0', "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI2",
             "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI5", "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDMy"]:
                continue

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
                hdf5_path = None

            yield SubtaskRecord(
                gt_id=gt_id,
                hazard_calc_id=task_id,
                image=latest_engine_image,
                config_hash=config_hash,
                hdf5_path=hdf5_path,
                vs30=jobconf.config.get('site_params', 'reference_vs30_value'),
            )

    def get_hazard_task_ids(query_res):
        for edge in query_res['children']['edges']:
            yield edge['node']['child']['id']


    for subtask_info in handle_subtasks(gt_id, get_hazard_task_ids(query_res)):
        if process_v3:
            ArgsRecord = collections.namedtuple('ArgsRecord', 
                'calc_id, source_tags, source_ids, toshi_hazard_id, toshi_gt_id, locations_id, verbose, meta_data_only'
            )       
            args = ArgsRecord(
                calc_id=subtask_info.hdf5_path,
                toshi_gt_id=subtask_info.gt_id,
                toshi_hazard_id=subtask_info.hazard_calc_id,
                source_tags = "",
                source_ids = "",
                locations_id = "",
                verbose=verbose,
                meta_data_only=False
            )
            extract_and_save(args)
        else:
            compatible_calc = get_compatible_calc(compatible_calc_fk.split("_"))
            if compatible_calc is None:
                raise ValueError(f'compatible_calc: {compatible_calc_fk} was not found')
            handle_import_subtask_rev4(subtask_info, partition, compatible_calc, verbose, update, with_rlzs, dry_run)
        #crash out after one subtask
        assert 0


if __name__ == "__main__":
    main()
