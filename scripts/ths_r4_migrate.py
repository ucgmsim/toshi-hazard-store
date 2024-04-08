# flake8: noqa

"""Console script for preparing to load NSHM hazard curves to new REV4 tables using General Task(s) and nzshm-model.

This is NSHM process specific, as it assumes the following:
 - hazard producer metadata is available from the NSHM toshi-api via **nshm-toshi-client** library
 - NSHM model characteristics are available in the **nzshm-model** library


"""
import datetime as dt
import logging
import os
import pathlib

# import time
import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

log = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)
logging.getLogger('nzshm_model').setLevel(logging.INFO)
logging.getLogger('gql.transport').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store.db_adapter.sqlite.sqlite_store').setLevel(logging.WARNING)


from nzshm_model.logic_tree.source_logic_tree.toshi_api import (  # noqa: E402 and this function be in the client !
    get_secret,
)

from toshi_hazard_store.config import DEPLOYMENT_STAGE as THS_STAGE
from toshi_hazard_store.config import LOCAL_CACHE_FOLDER, NUM_BATCH_WORKERS
from toshi_hazard_store.config import REGION as THS_REGION
from toshi_hazard_store.config import USE_SQLITE_ADAPTER

# from toshi_hazard_store import model
from toshi_hazard_store.model.revision_4 import hazard_models
from toshi_hazard_store.multi_batch import save_parallel
from toshi_hazard_store.oq_import import get_compatible_calc
from toshi_hazard_store.oq_import.migrate_v3_to_v4 import ECR_REPONAME, SubtaskRecord, migrate_realisations_from_subtask

from .core import echo_settings
from .revision_4 import aws_ecr_docker_image as aws_ecr
from .revision_4 import toshi_api_client  # noqa: E402
from .revision_4 import oq_config

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


def process_gt_subtasks(gt_id: str, work_folder: str, verbose: bool = False):
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

        # # completed already
        # if task_id in ['T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3', 'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3',
        #     'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE4', 'T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI0',
        #     "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI2",
        #     "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDMy", "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI5"]:
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

        yield SubtaskRecord(
            gt_id=gt_id,
            hazard_calc_id=query_res['hazard_solution']['id'],
            image=latest_engine_image,
            config_hash=config_hash,
            vs30=jobconf.config.get('site_params', 'reference_vs30_value'),
        )


#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|

# @click.group()
# @click.pass_context
# def main(context, work_folder):
#     """Import NSHM Model hazard curves to new revision 4 models."""

#     context.ensure_object(dict)
#     context.obj['work_folder'] = work_folder


@click.command()
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
    help="set the source store. defaults to LOCAL",
)
@click.option(
    '--target',
    '-T',
    type=click.Choice(['AWS', 'LOCAL', 'ARROW'], case_sensitive=False),
    default='LOCAL',
    help="set the target store. defaults to LOCAL",
)
@click.option('-W', '--work_folder', default=lambda: os.getcwd(), help="defaults to Current Working Directory")
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
def main(
    gt_id,
    partition,
    compat_calc,
    update,
    source,
    target,
    work_folder,
    verbose,
    dry_run,
):
    """Migrate realisations from V3 to R4 table for GT_ID, PARTITION and COMPAT_CALC

    GT_ID is an NSHM General task id containing HazardAutomation Tasks\n
    PARTITION is a table partition (hash) for Producer\n
    COMPAT is foreign key of the compatible_calc in form `A_B`
    """

    compatible_calc = get_compatible_calc(compat_calc.split("_"))
    if compatible_calc is None:
        raise ValueError(f'compatible_calc: {compat_calc} was not found')

    if verbose:
        echo_settings(work_folder)
        click.echo()
        click.echo('fetching General Task subtasks')

    def generate_models():
        task_count = 0
        for subtask_info in process_gt_subtasks(gt_id, work_folder=work_folder, verbose=verbose):
            task_count += 1
            # if task_count < 7:
            #     continue

            log.info(f"Processing calculation {subtask_info.hazard_calc_id} in gt {gt_id}")
            count = 0
            for new_rlz in migrate_realisations_from_subtask(
                subtask_info, source, partition, compatible_calc, verbose, update, dry_run=False
            ):
                count += 1
                yield new_rlz
            log.info(f"Produced {count} source objects from {subtask_info.hazard_calc_id} in {gt_id}")
            # crash out after some subtasks..
            if task_count >= 12:
                break

    def chunked(iterable, chunk_size=100):
        count = 0
        chunk = []
        for item in iterable:
            chunk.append(item)
            count +=1
            if count % chunk_size == 0:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    if dry_run:
        for itm in generate_models():
            pass
        log.info("Dry run completed")
    elif target == 'ARROW':
        arrow_folder = pathlib.Path(work_folder) / 'ARROW'

        def batch_builder(table_size):
            n = 0
            for chunk in chunked(generate_models(), chunk_size=table_size):
                df = pd.DataFrame([rlz.to_simple_dict() for rlz in chunk])
                yield df # pa.Table.from_pandas(df)
                n+=1
                log.info(f"built dataframe {n}")

        hrc_schema = pa.schema([
            ('created', pa.timestamp('ms', tz='UTC')),
            ('compatible_calc_fk', pa.string()),
            ('producer_config_fk', pa.string()),
            ('calculation_id', pa.string()),
            ('values', pa.list_(pa.float32(), 44)),
            ('imt', pa.string()),
            ('vs30', pa.uint16()),
            # ('site_vs30', pa.uint16()),
            ('source_digests', pa.list_(pa.string(), -1)),
            ('gmm_digests', pa.list_(pa.string(), -1)),
            ('nloc_001', pa.string()),
            ('partition_key', pa.string()),
            ('sort_key', pa.string())
        ])

        with pa.OSFile(f'{arrow_folder}/bigfile.arrow', 'wb') as sink:
            with pa.ipc.new_file(sink, hrc_schema) as writer:
                for table in batch_builder(10000):
                    batch = pa.record_batch(table, hrc_schema)
                    writer.write(batch)

        """
        >>> reader = pa.ipc.open_file(open('WORKING/ARROW/bigfile.arrow', 'rb'))
        >>> reader
        <pyarrow.ipc.RecordBatchFileReader object at 0x71fc83f705c0>
        >>> df = reader.read_pandas()
        """
        # ds.write_dataset(scanner(), str(arrow_folder), format="parquet",
        #     partitioning=ds.partitioning(pa.schema([("range_key", pa.string())]))
        # )

    else:
        workers = 1 if target == 'LOCAL' else NUM_BATCH_WORKERS
        batch_size = 100 if target == 'LOCAL' else 25
        model = hazard_models.HazardRealizationCurve
        save_parallel("", generate_models(), model, workers, batch_size)


if __name__ == "__main__":
    main()
