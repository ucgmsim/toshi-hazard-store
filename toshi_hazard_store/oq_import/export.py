import json
import logging
import random

# from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

from toshi_hazard_store import model
from toshi_hazard_store.config import NUM_BATCH_WORKERS, USE_SQLITE_ADAPTER
from toshi_hazard_store.model.revision_4 import hazard_models
from toshi_hazard_store.multi_batch import save_parallel
from toshi_hazard_store.transform import parse_logic_tree_branches
from toshi_hazard_store.utils import normalise_site_code

# import pandas as pd


log = logging.getLogger(__name__)

NUM_BATCH_WORKERS = 1 if USE_SQLITE_ADAPTER else NUM_BATCH_WORKERS
BATCH_SIZE = 1000 if USE_SQLITE_ADAPTER else random.randint(15, 50)


def create_producer_config(
    partition_key: str,
    compatible_calc_fk: Tuple[str, str],
    producer_software: str,
    producer_version_id: str,
    configuration_hash: str,
    configuration_data: Optional[str],
    notes: Optional[str],
    dry_run: bool = False,
) -> 'hazard_models.HazardCurveProducerConfig':
    # first check the Foreign Key is OK
    mCHC = hazard_models.CompatibleHazardCalculation

    assert len(compatible_calc_fk) == 2

    log.info(f'checking compatible_calc_fk {compatible_calc_fk}')
    assert next(mCHC.query(compatible_calc_fk[0], mCHC.uniq_id == compatible_calc_fk[1]))
    mHCPC = hazard_models.HazardCurveProducerConfig

    m = mHCPC(
        partition_key=partition_key,
        compatible_calc_fk=compatible_calc_fk,
        producer_software=producer_software,
        producer_version_id=producer_version_id,
        configuration_hash=configuration_hash,
        configuration_data=configuration_data,
        notes=notes,
    )
    m.range_key = f"{producer_software}:{producer_version_id}:{configuration_hash}"
    if not dry_run:
        m.save()
    return m


def export_rlzs_rev4(
    extractor,
    compatible_calc_fk: Tuple[str, str],
    producer_config_fk: Tuple[str, str],
    vs30: int,
    hazard_calc_id: str,
    return_rlz=True,
) -> Union[List[hazard_models.HazardRealizationCurve], None]:

    # first check the FKs are OK
    mCHC = hazard_models.CompatibleHazardCalculation
    mHCPC = hazard_models.HazardCurveProducerConfig

    assert len(compatible_calc_fk) == 2
    assert len(producer_config_fk) == 2

    log.info(f'checking compatible_calc_fk {compatible_calc_fk}')
    assert next(mCHC.query(compatible_calc_fk[0], mCHC.uniq_id == compatible_calc_fk[1]))
    log.info(f'checking producer_config_fk {producer_config_fk}')
    pc = next(
        mHCPC.query(
            producer_config_fk[0],
            mHCPC.range_key == producer_config_fk[1],
            mHCPC.compatible_calc_fk == compatible_calc_fk,  # filter_condition
        )
    )
    assert pc
    # log.debug(str(pc))
    # log.debug(str(pc.compatible_calc_fk))

    oq = json.loads(extractor.get('oqparam').json)
    sites = extractor.get('sitecol').to_dframe()
    rlzs = extractor.get('hcurves?kind=rlzs', asdict=True)

    rlz_keys = [k for k in rlzs.keys() if 'rlz-' in k]
    imtls = oq['hazard_imtls']  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}

    # oq = json.loads(extractor.get('oqparam').json)
    source_lt, gsim_lt, rlz_lt = parse_logic_tree_branches(extractor)

    log.debug('rlz %s' % rlz_lt)
    log.debug('src %s' % source_lt)
    log.debug('gsim %s' % gsim_lt)

    # assert 0

    def generate_models():
        for i_site in range(len(sites)):
            loc = normalise_site_code((sites.loc[i_site, 'lon'], sites.loc[i_site, 'lat']), True)
            # print(f'loc: {loc}')
            for i_rlz, rlz in enumerate(rlz_keys):

                values = []
                for i_imt, imt in enumerate(imtls.keys()):
                    values.append(
                        model.IMTValuesAttribute(
                            imt=imt,
                            lvls=imtls[imt],
                            vals=rlzs[rlz][i_site][i_imt].tolist(),
                        )
                    )
                oq_realization = hazard_models.HazardRealizationCurve(
                    compatible_calc_fk=compatible_calc_fk,
                    producer_config_fk=producer_config_fk,
                    calculation_id=hazard_calc_id,
                    values=values,
                    rlz=rlz,
                    vs30=vs30,
                )
                # if oqmeta.model.vs30 == 0:
                #    oq_realization.site_vs30 = sites.loc[i_site, 'vs30']
                yield oq_realization.set_location(loc)

    # used for testing
    if return_rlz:
        return list(generate_models())

    save_parallel("", generate_models(), hazard_models.HazardRealizationCurve, NUM_BATCH_WORKERS, BATCH_SIZE)
    return None
