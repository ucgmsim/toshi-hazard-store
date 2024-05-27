import json
import logging
import math
import random
from dataclasses import dataclass

import pandas as pd

from toshi_hazard_store import model
from toshi_hazard_store.config import NUM_BATCH_WORKERS, USE_SQLITE_ADAPTER
from toshi_hazard_store.model import openquake_models
from toshi_hazard_store.multi_batch import save_parallel
from toshi_hazard_store.transform import parse_logic_tree_branches
from toshi_hazard_store.utils import normalise_site_code

log = logging.getLogger(__name__)

NUM_BATCH_WORKERS = 1 if USE_SQLITE_ADAPTER else NUM_BATCH_WORKERS
BATCH_SIZE = 1000 if USE_SQLITE_ADAPTER else random.randint(15, 50)


@dataclass
class OpenquakeMeta:
    source_lt: pd.DataFrame
    gsim_lt: pd.DataFrame
    rlz_lt: pd.DataFrame
    model: openquake_models.ToshiOpenquakeMeta


def export_meta_v3(extractor, toshi_hazard_id, toshi_gt_id, locations_id, source_tags, source_ids):
    """Extract and same the meta data."""
    oq = json.loads(extractor.get('oqparam').json)
    source_lt, gsim_lt, rlz_lt = parse_logic_tree_branches(extractor)

    df_len = 0
    df_len += len(source_lt.to_json())
    df_len += len(gsim_lt.to_json())
    df_len += len(rlz_lt.to_json())

    if df_len >= 300e3:
        log.warning('WARNING: Dataframes for this job may be too large to store on DynamoDB.')

    vs30 = oq['reference_vs30_value']

    if math.isnan(vs30):
        vs30 = 0

    log.debug(f'vs30: {vs30}')

    obj = openquake_models.ToshiOpenquakeMeta(
        partition_key="ToshiOpenquakeMeta",
        hazard_solution_id=toshi_hazard_id,
        general_task_id=toshi_gt_id,
        hazsol_vs30_rk=f"{toshi_hazard_id}:{str(int(vs30)).zfill(3)}",
        # updated=dt.datetime.now(tzutc()),
        # known at configuration
        vs30=int(vs30),  # vs30 value
        imts=list(oq['hazard_imtls'].keys()),  # list of IMTs
        locations_id=locations_id,  # Location code or list ID
        source_tags=source_tags,
        source_ids=source_ids,
        inv_time=oq['investigation_time'],
        src_lt=source_lt.to_json(),  # sources meta as DataFrame JSON
        gsim_lt=gsim_lt.to_json(),  # gmpe meta as DataFrame JSON
        rlz_lt=rlz_lt.to_json(),  # realization meta as DataFrame JSON
    )
    obj.save()
    return OpenquakeMeta(source_lt, gsim_lt, rlz_lt, obj)


def export_rlzs_v3(extractor, oqmeta: OpenquakeMeta, return_rlz=False):
    oq = json.loads(extractor.get('oqparam').json)
    sites = extractor.get('sitecol').to_dframe()
    rlzs = extractor.get('hcurves?kind=rlzs', asdict=True)

    rlz_keys = [k for k in rlzs.keys() if 'rlz-' in k]
    imtls = oq['hazard_imtls']  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}

    log.debug('rlz %s' % oqmeta.rlz_lt)
    log.debug('src %s' % oqmeta.source_lt)
    log.debug('gsim %s' % oqmeta.gsim_lt)

    def generate_models():
        count = 0
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
                oq_realization = openquake_models.OpenquakeRealization(
                    values=values,
                    rlz=i_rlz,
                    vs30=oqmeta.model.vs30,
                    hazard_solution_id=oqmeta.model.hazard_solution_id,
                    source_tags=oqmeta.model.source_tags,
                    source_ids=oqmeta.model.source_ids,
                )
                if oqmeta.model.vs30 == 0:
                    oq_realization.site_vs30 = sites.loc[i_site, 'vs30']
                yield oq_realization.set_location(loc)
                count += 1

        log.debug(f'generate_models() produced {count} models.')

    # used for testing
    if return_rlz:
        return list(generate_models())

    save_parallel("", generate_models(), openquake_models.OpenquakeRealization, NUM_BATCH_WORKERS, BATCH_SIZE)
