from dataclasses import dataclass

import pandas as pd

from toshi_hazard_store import model
from toshi_hazard_store.config import NUM_BATCH_WORKERS
from toshi_hazard_store.multi_batch import save_parallel
from toshi_hazard_store.transform import parse_logic_tree_branches
from toshi_hazard_store.utils import normalise_site_code

try:
    from openquake.calculators.export.hazard import get_sites
except ImportError:
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
    raise


@dataclass
class OpenquakeMeta:
    source_lt: pd.DataFrame
    gsim_lt: pd.DataFrame
    rlz_lt: pd.DataFrame
    model: model.ToshiOpenquakeMeta


def export_meta_v3(dstore, toshi_hazard_id, toshi_gt_id, locations_id, source_tags, source_ids):
    """Extract and same the meta data."""
    oq = dstore['oqparam']
    source_lt, gsim_lt, rlz_lt = parse_logic_tree_branches(dstore.filename)

    df_len = 0
    df_len += len(source_lt.to_json())
    df_len += len(gsim_lt.to_json())
    df_len += len(rlz_lt.to_json())

    if df_len >= 300e3:
        print('WARNING: Dataframes for this job may be too large to store on DynamoDB.')

    obj = model.ToshiOpenquakeMeta(
        partition_key="ToshiOpenquakeMeta",
        hazard_solution_id=toshi_hazard_id,
        general_task_id=toshi_gt_id,
        hazsol_vs30_rk=f"{toshi_hazard_id}:{str(int(oq.reference_vs30_value)).zfill(3)}",
        # updated=dt.datetime.now(tzutc()),
        # known at configuration
        vs30=int(oq.reference_vs30_value),  # vs30 value
        imts=list(oq.imtls.keys()),  # list of IMTs
        locations_id=locations_id,  # Location code or list ID
        source_tags=source_tags,
        source_ids=source_ids,
        inv_time=vars(oq)['investigation_time'],
        src_lt=source_lt.to_json(),  # sources meta as DataFrame JSON
        gsim_lt=gsim_lt.to_json(),  # gmpe meta as DataFrame JSON
        rlz_lt=rlz_lt.to_json(),  # realization meta as DataFrame JSON
    )
    obj.save()
    return OpenquakeMeta(source_lt, gsim_lt, rlz_lt, obj)


def export_rlzs_v3(dstore, oqmeta: OpenquakeMeta):
    oq = dstore['oqparam']
    sitemesh = get_sites(dstore['sitecol'])

    n_sites, n_rlzs, n_lvls, n_vals = dstore['hcurves-rlzs'].shape
    imtls = oq.imtls  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}
    imtl_keys = list(oq.imtls.keys())

    print('rlz', oqmeta.rlz_lt)
    print()
    print('src', oqmeta.source_lt)
    print()
    print('gsim', oqmeta.gsim_lt)
    print()

    def generate_models():
        for site in range(n_sites):
            loc = normalise_site_code(sitemesh[site], True)
            # print(f'loc: {loc}')
            for rlz in range(n_rlzs):

                values = []
                for lvl in range(n_lvls):
                    values.append(
                        model.IMTValuesAttribute(
                            imt=imtl_keys[lvl],
                            lvls=imtls[imtl_keys[lvl]],
                            vals=dstore['hcurves-rlzs'][site][rlz][lvl].tolist(),
                        )
                    )
                rlz = model.OpenquakeRealization(
                    values=values,
                    rlz=rlz,
                    vs30=oqmeta.model.vs30,
                    hazard_solution_id=oqmeta.model.hazard_solution_id,
                    source_tags=oqmeta.model.source_tags,
                    source_ids=oqmeta.model.source_ids,
                )
                rlz.set_location(loc)
                yield rlz

    save_parallel("", generate_models(), model.OpenquakeRealization, NUM_BATCH_WORKERS)
