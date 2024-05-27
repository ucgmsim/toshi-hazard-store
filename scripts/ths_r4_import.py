"""
Console script for preparing to load NSHM hazard curves to new REV4 tables using General Task(s)
and the nzshm-model python library.

The use case for this is reprocessing a set of hazard outputs produced by the NSHM hazards pipeline.

NSHM specific prerequisites are:
    - that hazard producer metadata is available from the NSHM toshi-api via **nshm-toshi-client** library
    - NSHM model characteristics are available in the **nzshm-model** library

Process outline:
    - Given a general task containing hazard calcs used in NHSM, we want to iterate over the sub-tasks and do
      the setup required for importing the hazard curves:
        - pull the configs and check we have a compatible producer config (or ...) cmd `producers`
        - optionally create new producer configs automatically, and record info about these
    - if new producer configs are created, then it is the users responsibility to assign
      a CompatibleCalculation to each
    - Hazard curves are acquired either:
        - directly form the original HDF5 files stored in Toshi API
        - from V3 RealisationCurves stored as PynamoDB records (dynamodb or sqlite3)
    - Hazard curves are output as either:
        - new THS Rev4 PynamoDB records (dynamodb or sqlite3).
        - directly to a parquet dataset (ARROW options). Thsi is the newest/fastest option.

"""

import collections
import datetime as dt
import logging
import os
import pathlib
from typing import Iterable

import click

import toshi_hazard_store  # noqa: E402
from toshi_hazard_store.model.revision_4 import extract_classical_hdf5, hazard_models, pyarrow_dataset
from toshi_hazard_store.model.revision_4.migrate_v3_to_v4 import ECR_REGISTRY_ID, ECR_REPONAME
from toshi_hazard_store.oq_import import (  # noqa: E402
    create_producer_config,
    export_rlzs_rev4,
    get_compatible_calc,
    get_producer_config,
)

from .revision_4 import aws_ecr_docker_image as aws_ecr
from .revision_4 import toshi_api_client  # noqa: E402
from .revision_4 import oq_config

try:
    from openquake.calculators.extract import Extractor
except (ModuleNotFoundError, ImportError):
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")

logging.basicConfig(level=logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)
logging.getLogger('nzshm_model').setLevel(logging.INFO)
logging.getLogger('gql.transport').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('root').setLevel(logging.INFO)

log = logging.getLogger(__name__)


API_URL = os.getenv('NZSHM22_TOSHI_API_URL', "http://127.0.0.1:5000/graphql")
API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY', "")
S3_URL = None

# DEPLOYMENT_STAGE = os.getenv('DEPLOYMENT_STAGE', 'LOCAL').upper()
REGION = os.getenv('REGION', 'ap-southeast-2')  # SYDNEY

SubtaskRecord = collections.namedtuple('SubtaskRecord', 'gt_id, hazard_calc_id, config_hash, image, hdf5_path, vs30')


def handle_import_subtask_rev4(
    subtask_info: 'SubtaskRecord',
    partition,
    compatible_calc,
    target,
    output_folder,
    verbose,
    update,
    with_rlzs,
    dry_run=False,
):
    if verbose:
        click.echo(subtask_info)

    extractor = None

    producer_software = f"{ECR_REGISTRY_ID}/{ECR_REPONAME}"
    producer_version_id = subtask_info.image['imageDigest'][7:27]  # first 20 bits of hashdigest
    configuration_hash = subtask_info.config_hash
    pc_key = (partition, f"{producer_software}:{producer_version_id}:{configuration_hash}")

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
            click.echo(
                f"New Model {producer_config} has foreign key ({producer_config.partition_key},"
                f" {producer_config.range_key})"
            )

    if with_rlzs:
        if target == 'ARROW':
            # this uses the direct to parquet dataset exporter, approx 100times faster
            model_generator = extract_classical_hdf5.rlzs_to_record_batch_reader(
                hdf5_file=str(subtask_info.hdf5_path),
                calculation_id=subtask_info.hazard_calc_id,
                compatible_calc_fk=compatible_calc.foreign_key()[1],  # TODO DROPPING the partition = awkward!
                producer_config_fk=producer_config.foreign_key()[1],  # DROPPING the partition
            )
            pyarrow_dataset.append_models_to_dataset(model_generator, output_folder)
        else:
            # this uses the pynamodb model exporter
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
            print(f"exported all models in {subtask_info.hdf5_path.parent.name} to {target}")


def handle_subtasks(
    gt_id: str,
    gtapi: toshi_api_client.ApiClient,
    subtask_ids: Iterable,
    work_folder: str,
    with_rlzs: bool,
    verbose: bool,
):

    subtasks_folder = pathlib.Path(work_folder, gt_id, 'subtasks')
    subtasks_folder.mkdir(parents=True, exist_ok=True)

    if verbose:
        click.echo('fetching ECR stash')

    ecr_repo_stash = aws_ecr.ECRRepoStash(
        ECR_REPONAME, oldest_image_date=dt.datetime(2023, 3, 20, tzinfo=dt.timezone.utc)
    ).fetch()

    for task_id in subtask_ids:

        # completed already
        # if task_id in ['T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3', 'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3']:
        #     continue

        # # problems
        # if task_id in ['T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE4', 'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI0',
        #  "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI2",
        #  "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDMy"]: # "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI5",
        #     continue

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

        if with_rlzs:
            hdf5_path = oq_config.process_hdf5(gtapi, task_id, query_res, subtasks_folder, manipulate=True)
        else:
            hdf5_path = None

        yield SubtaskRecord(
            gt_id=gt_id,
            hazard_calc_id=query_res['hazard_solution']['id'],
            image=latest_engine_image,
            config_hash=config_hash,
            hdf5_path=hdf5_path,
            vs30=jobconf.config.get('site_params', 'reference_vs30_value'),
        )


#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|
#
@click.group()
def main():
    """Import NSHM Model hazard curves to new revision 4 models."""


@main.command()
def create_tables():
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
    '-T',
    '--target',
    type=click.Choice(['AWS', 'LOCAL', 'ARROW'], case_sensitive=False),
    default='LOCAL',
    help="set the target store. defaults to LOCAL. ARROW produces parquet instead of dynamoDB tables",
)
@click.option('-W', '--work_folder', default=lambda: os.getcwd(), help="defaults to current directory")
@click.option(
    '-O',
    '--output_folder',
    type=click.Path(path_type=pathlib.Path, exists=False),
    help="arrow target folder (only used with `-T ARROW`",
)
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
def producers(
    # model_id,
    gt_id,
    partition,
    target,
    work_folder,
    output_folder,
    compatible_calc_fk,
    update,
    with_rlzs,
    # process_v3,
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

    # if verbose:
    #    echo_settings(work_folder)

    headers = {"x-api-key": API_KEY}
    gtapi = toshi_api_client.ApiClient(API_URL, None, with_schema_validation=False, headers=headers)

    if verbose:
        click.echo('fetching General Task subtasks')

    def get_hazard_task_ids(query_res):
        for edge in query_res['children']['edges']:
            yield edge['node']['child']['id']

    # query the API for general task and
    query_res = gtapi.get_gt_subtasks(gt_id)

    count = 0
    for subtask_info in handle_subtasks(gt_id, gtapi, get_hazard_task_ids(query_res), work_folder, with_rlzs, verbose):

        count += 1
        if dry_run:
            click.echo(f'DRY RUN. otherwise, would be processing subtask {count} {subtask_info} ')
            continue

        # normal processing
        compatible_calc = get_compatible_calc(compatible_calc_fk.split("_"))
        # print("CC ", compatible_calc)
        if compatible_calc is None:
            raise ValueError(f'compatible_calc: {compatible_calc_fk} was not found')
        handle_import_subtask_rev4(
            subtask_info, partition, compatible_calc, target, output_folder, verbose, update, with_rlzs, dry_run
        )


if __name__ == "__main__":
    main()
