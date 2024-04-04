"""Migrate all the realisations for the given subtask"""

import collections
import logging
import pandas
import time
import importlib
import sys

from typing import Iterator
from nzshm_common.grids import load_grid
from nzshm_common.location.code_location import CodedLocation

import toshi_hazard_store.model

from toshi_hazard_store.oq_import import create_producer_config, get_producer_config
from toshi_hazard_store.oq_import.oq_manipulate_hdf5 import migrate_nshm_uncertainty_string
from toshi_hazard_store.oq_import.parse_oq_realizations import rlz_mapper_from_dataframes

SubtaskRecord = collections.namedtuple(
    'SubtaskRecord', 'gt_id, hazard_calc_id, config_hash, image, vs30'
)

ECR_REGISTRY_ID = '461564345538.dkr.ecr.us-east-1.amazonaws.com'
ECR_REPONAME = "nzshm22/runzi-openquake"

log = logging.getLogger(__name__)


def migrate_realisations_from_subtask(
    subtask_info: 'SubtaskRecord', source:str, partition:str, compatible_calc, verbose, update, dry_run=False
) ->Iterator[toshi_hazard_store.model.openquake_models.OpenquakeRealization]:
    """Migrate all the realisations for the given subtask
    """
    if source == 'AWS':
        # set tables to default classes
        importlib.reload(sys.modules['toshi_hazard_store.model.location_indexed_model'])
        importlib.reload(sys.modules['toshi_hazard_store.model.openquake_models'])
    elif source == 'LOCAL':
        pass
        # configure_v3_source(SqliteAdapter)
    else:
        raise ValueError('unknown source {source}')

    if verbose:
        log.info(subtask_info)

    producer_software = f"{ECR_REGISTRY_ID}/{ECR_REPONAME}"
    producer_version_id = subtask_info.image['imageDigest'][7:27]  # first 20 bits of hashdigest
    configuration_hash = subtask_info.config_hash
    pc_key = (partition, f"{producer_software}:{producer_version_id}:{configuration_hash}")

    # check for existing
    producer_config = get_producer_config(pc_key, compatible_calc)
    if producer_config:
        if verbose:
            log.info(f'found producer_config {pc_key} ')
        # if update:
        #     producer_config.notes = "notes 2"
        #     producer_config.save()
        #     log.info(f'updated producer_config {pc_key} ')

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

        log.info(f"New Model {producer_config} has foreign key ({producer_config.partition_key}, {producer_config.range_key})")

    mRLZ_V4 = toshi_hazard_store.model.revision_4.hazard_models.HazardRealizationCurve

    # table classes may be rebased, this makes sure we always get the current class definition
    mRLZ_V3 = toshi_hazard_store.model.openquake_models.__dict__['OpenquakeRealization']
    mMeta   = toshi_hazard_store.model.openquake_models.__dict__['ToshiOpenquakeMeta']

    # # modify the source region
    # mMeta.Meta.region = 'ap-southeast-25'
    # mRLZ_V3.Meta.region = 'ap-southeast-25'

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

    grid = load_grid('NZ_0_1_NB_1_1')

    for location in [CodedLocation(o[0], o[1], 0.1) for o in grid]:
        for source_rlz in mRLZ_V3.query(
            location.code,
            mRLZ_V3.sort_key >= location.resample(0.001).code,
            filter_condition=(mRLZ_V3.hazard_solution_id == subtask_info.hazard_calc_id) & (mRLZ_V3.vs30 == subtask_info.vs30)
            ):

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

