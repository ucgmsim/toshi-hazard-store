"""Migrate all the realisations for the given subtask"""

import collections
import importlib
import logging
import sys
from typing import Iterator

import pandas
from nzshm_common.grids import get_location_grid
from nzshm_common.location import coded_location, location

import toshi_hazard_store.model
from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter
from toshi_hazard_store.oq_import import create_producer_config, get_producer_config
from toshi_hazard_store.oq_import.oq_manipulate_hdf5 import migrate_nshm_uncertainty_string
from toshi_hazard_store.oq_import.parse_oq_realizations import rlz_mapper_from_dataframes

SubtaskRecord = collections.namedtuple('SubtaskRecord', 'gt_id, hazard_calc_id, config_hash, image, vs30')

ECR_REGISTRY_ID = '461564345538.dkr.ecr.us-east-1.amazonaws.com'
ECR_REPONAME = "nzshm22/runzi-openquake"

log = logging.getLogger(__name__)


def migrate_realisations_from_subtask(
    subtask_info: 'SubtaskRecord',
    source: str,
    partition: str,
    compatible_calc,
    verbose,
    update,
    dry_run=False,
    bail_after=None,
) -> Iterator[toshi_hazard_store.model.openquake_models.OpenquakeRealization]:
    """
    Migrate all the realisations for the given subtask
    """
    if source == 'AWS':
        # set tables to default classes
        importlib.reload(sys.modules['toshi_hazard_store.model.location_indexed_model'])
        importlib.reload(sys.modules['toshi_hazard_store.model.openquake_models'])
    elif source == 'LOCAL':
        adapter_model = SqliteAdapter
        log.info(f"Configure adapter: {adapter_model}")
        ensure_class_bases_begin_with(
            namespace=toshi_hazard_store.model.openquake_models.__dict__,
            class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
            base_class=adapter_model,
        )
        ensure_class_bases_begin_with(
            namespace=toshi_hazard_store.model.location_indexed_model.__dict__,
            class_name=str('LocationIndexedModel'),
            base_class=adapter_model,
        )
        ensure_class_bases_begin_with(
            namespace=toshi_hazard_store.model.openquake_models.__dict__,
            class_name=str('OpenquakeRealization'),  # `str` type differs on Python 2 vs. 3.
            base_class=adapter_model,
        )
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

        log.info(
            f"New Model {producer_config} has foreign key ({producer_config.partition_key}, "
            f"{producer_config.range_key})"
        )

    mRLZ_V4 = toshi_hazard_store.model.revision_4.hazard_models.HazardRealizationCurve

    # table classes may be rebased, this makes sure we always get the current class definition
    mRLZ_V3 = toshi_hazard_store.model.openquake_models.__dict__['OpenquakeRealization']
    mMeta = toshi_hazard_store.model.openquake_models.__dict__['ToshiOpenquakeMeta']

    # # modify the source region
    # mMeta.Meta.region = 'ap-southeast-25'
    # mRLZ_V3.Meta.region = 'ap-southeast-25'

    # Get the V3 Metadata ...
    query = mMeta.query(
        "ToshiOpenquakeMeta", mMeta.hazsol_vs30_rk == f"{subtask_info.hazard_calc_id}:{subtask_info.vs30}"
    )

    try:
        meta = next(query)
    except StopIteration:
        log.warning(
            f"Metadata for {subtask_info.hazard_calc_id}:{subtask_info.vs30} was not found. Terminating migration."
        )
        return

    gsim_lt = pandas.read_json(meta.gsim_lt)
    source_lt = pandas.read_json(meta.src_lt)
    rlz_lt = pandas.read_json(meta.rlz_lt)

    # apply gsim migrations
    gsim_lt["uncertainty"] = gsim_lt["uncertainty"].map(migrate_nshm_uncertainty_string)

    # build the realisation mapper
    rlz_map = rlz_mapper_from_dataframes(source_lt=source_lt, gsim_lt=gsim_lt, rlz_lt=rlz_lt)

    # using new binned locations from nzshm-common#pre-release
    nz1_grid = get_location_grid('NZ_0_1_NB_1_1', 0.1)
    location_list = set(nz1_grid + location.get_location_list(["NZ", "SRWG214"]))
    partition_codes = coded_location.bin_locations(location_list, at_resolution=0.1)

    processed_count = 0
    yield_count = 0
    for partition_code in partition_codes:
        result = mRLZ_V3.query(
            partition_code,
            mRLZ_V3.sort_key >= partition_code[:3],
            filter_condition=(mRLZ_V3.nloc_1 == partition_code)
            & (mRLZ_V3.hazard_solution_id == subtask_info.hazard_calc_id),
        )
        for source_rlz in result:
            realization = rlz_map[source_rlz.rlz]
            for imt_values in source_rlz.values:
                log.debug(realization)
                target_realization = mRLZ_V4(
                    compatible_calc_fk=compatible_calc.foreign_key(),
                    producer_config_fk=producer_config.foreign_key(),
                    created=source_rlz.created,
                    calculation_id=subtask_info.hazard_calc_id,
                    values=list(imt_values.vals),
                    imt=imt_values.imt,
                    vs30=source_rlz.vs30,
                    # site_vs30=source_rlz.site_vs30,
                    sources_digest=realization.sources.hash_digest,
                    gmms_digest=realization.gmms.hash_digest,
                )
                yield target_realization.set_location(
                    coded_location.CodedLocation(lat=source_rlz.lat, lon=source_rlz.lon, resolution=0.001)
                )
                yield_count += 1

            processed_count += 1

            if bail_after and processed_count >= bail_after:
                log.warning(f'bailing after creating {yield_count} new rlz from {processed_count} source realisations')
                return
