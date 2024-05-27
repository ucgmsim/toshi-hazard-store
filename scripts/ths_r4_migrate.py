"""
Console script for preparing to load NSHM hazard curves to new REV4 tables using General Task(s) and nzshm-model.

WARNING:
 - maybe this module will be deprecated... It seems there's a lot of overlap with ./ths_r4_import.py

This is NSHM process specific, as it assumes the following:
 - hazard producer metadata is available from the NSHM toshi-api via **nshm-toshi-client** library
 - NSHM model characteristics are available in the **nzshm-model** library

"""

import datetime as dt
import logging
import os
import pathlib

import click
import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv

from toshi_hazard_store.model.revision_4 import hazard_models, pyarrow_dataset
from toshi_hazard_store.model.revision_4.migrate_v3_to_v4 import (
    ECR_REPONAME,
    SubtaskRecord,
    migrate_realisations_from_subtask,
)
from toshi_hazard_store.multi_batch import save_parallel
from toshi_hazard_store.oq_import import get_compatible_calc

from .core import echo_settings
from .revision_4 import aws_ecr_docker_image as aws_ecr
from .revision_4 import toshi_api_client  # noqa: E402
from .revision_4 import oq_config

# from toshi_hazard_store.config import DEPLOYMENT_STAGE as THS_STAGE
# from toshi_hazard_store.config import USE_SQLITE_ADAPTER, SQLITE_ADAPTER_FOLDER
# from toshi_hazard_store.model.revision_4 import extract_classical_hdf5

log = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)
logging.getLogger('nzshm_model').setLevel(logging.INFO)
logging.getLogger('gql.transport').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store.db_adapter.sqlite.sqlite_store').setLevel(logging.WARNING)

load_dotenv()  # take environment variables from .env.*

API_URL = os.getenv('NZSHM22_TOSHI_API_URL', "http://127.0.0.1:5000/graphql")
API_KEY = os.getenv('NZSHM22_TOSHI_API_KEY', "")
S3_URL = None
DEPLOYMENT_STAGE = os.getenv('DEPLOYMENT_STAGE', 'LOCAL').upper()
REGION = os.getenv('REGION', 'ap-southeast-2')  # SYDNEY

NUM_BATCH_WORKERS = 4


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
        log.info(f"task: {task_id} hash: {config_hash} gt: {gt_id}  hazard_id: {query_res['hazard_solution']['id']}")

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


@click.command()
@click.argument('gt_id')
@click.argument('partition')
@click.argument('compat_calc')
@click.option(
    '-U',
    '--update',
    is_flag=True,
    default=False,
    help="overwrite existing producer record (versioned table).",
)
@click.option(
    '-S',
    '--source',
    type=click.Choice(['AWS', 'LOCAL'], case_sensitive=False),
    default='LOCAL',
    help="set the source store. defaults to LOCAL",
)
@click.option(
    '-T',
    '--target',
    type=click.Choice(['AWS', 'LOCAL', 'ARROW'], case_sensitive=False),
    default='LOCAL',
    help="set the target store. defaults to LOCAL. ARROW does produces parquet instead of dynamoDB tables",
)
@click.option('-W', '--work_folder', default=lambda: os.getcwd(), help="defaults to Current Working Directory")
@click.option(
    '-O',
    '--output_folder',
    type=click.Path(path_type=pathlib.Path, exists=False),
    help="arrow target folder (only used with `-T ARROW`",
)
@click.option(
    '-fmt',
    '--dataset_format',
    type=click.Choice(['arrow', 'parquet']),
    default='parquet',
    help="arrow serialisation format",
)
@click.option(
    '-b', '--bail_after', type=int, default=0, help="finish after processing the given number of realisations"
)
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
    output_folder,
    dataset_format,
    bail_after,
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
        # found_start = False
        for subtask_info in process_gt_subtasks(gt_id, work_folder=work_folder, verbose=verbose):
            task_count += 1
            # if task_count < 7: # the subtask to start with
            #     continue

            # if subtask_info.hazard_calc_id == "T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODU2MA==":
            #     found_start = True

            # if not found_start:
            #     log.info(f"skipping {subtask_info.hazard_calc_id} in gt {gt_id}")
            #     continue

            log.info(f"Processing calculation {subtask_info.hazard_calc_id} in gt {gt_id}")
            count = 0
            for new_rlz in migrate_realisations_from_subtask(
                subtask_info, source, partition, compatible_calc, verbose, update, dry_run=False
            ):
                count += 1
                # print(new_rlz.to_simple_dict())
                yield new_rlz
                # if count >= 1000:
                #     break
            log.info(f"Produced {count} source objects from {subtask_info.hazard_calc_id} in {gt_id}")
            # crash out after some subtasks..
            # if task_count >= 1: # 12:
            #     break

    if dry_run:
        for itm in generate_models():
            pass
        log.info("Dry run completed")
    elif target == 'ARROW':
        assert output_folder.parent.exists() & output_folder.parent.is_dir(), "check we have a good output folder"
        task_count = 0
        rlz_count = 0
        for subtask_info in process_gt_subtasks(gt_id, work_folder=work_folder, verbose=verbose):
            task_count += 1
            log.info(f"Processing calculation {subtask_info.hazard_calc_id} in gt {gt_id} using {target}")

            if target == 'ARROW':
                model_generator = migrate_realisations_from_subtask(
                    subtask_info,
                    source,
                    partition,
                    compatible_calc,
                    verbose,
                    update,
                    dry_run=False,
                    bail_after=bail_after,
                )

            models = [model.as_pandas_model() for model in model_generator]
            model_count = len(models)
            rlz_count += model_count
            log.info(f"Produced {model_count} source models from {subtask_info.hazard_calc_id} in {gt_id}")

            table = pa.Table.from_pandas(pd.DataFrame(models))
            pyarrow_dataset.append_models_to_dataset(table, output_folder, dataset_format)

            if bail_after and rlz_count >= bail_after:
                log.warning(f'bailing after creating {rlz_count} new rlz from {task_count} subtasks')
                break

    else:
        workers = 1 if target == 'LOCAL' else NUM_BATCH_WORKERS
        batch_size = 100 if target == 'LOCAL' else 25
        model = hazard_models.HazardRealizationCurve
        save_parallel("", generate_models(), model, workers, batch_size)


if __name__ == "__main__":
    main()
