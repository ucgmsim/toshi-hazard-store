"""
Convert openquake realizataions using nzshm_model.branch_registry
"""

import collections
import logging


from toshi_hazard_store.transform import parse_logic_tree_branches

from nzshm_model import branch_registry
from nzshm_model.psha_adapter.openquake import gmcm_branch_from_element_text

from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    import pandas
    from openquake.calculators.extract import Extractor

log = logging.getLogger(__name__)

registry = branch_registry.Registry()

RealizationRecord = collections.namedtuple('RealizationRecord', 'idx, path, sources, gmms')


def build_rlz_mapper(extractor: 'Extractor') -> Dict[int, RealizationRecord]:
    # extractor = Extractor(str(hdf5))
    source_lt, gsim_lt, rlz_lt = parse_logic_tree_branches(extractor)

    gmm_map = build_rlz_gmm_map(gsim_lt)
    source_map = build_rlz_source_map(source_lt)
    rlz_map = build_rlz_map(rlz_lt, source_map, gmm_map)
    return rlz_map


def build_rlz_gmm_map(gsim_lt: 'pandas.DataFrame') -> Dict[str, branch_registry.BranchRegistryEntry]:
    branch_ids = gsim_lt.branch.tolist()
    rlz_gmm_map = dict()
    for idx, uncertainty in enumerate(gsim_lt.uncertainty.tolist()):
        # handle GMM modifications ...
        if "Atkinson2022" in uncertainty:
            uncertainty += '\nmodified_sigma = "true"'
        if "AbrahamsonGulerce2020SInter" in uncertainty:
            uncertainty = uncertainty.replace("AbrahamsonGulerce2020SInter", "NZNSHM2022_AbrahamsonGulerce2020SInter")
        if "KuehnEtAl2020SInter" in uncertainty:
            uncertainty = uncertainty.replace("KuehnEtAl2020SInter", "NZNSHM2022_KuehnEtAl2020SInter")
            uncertainty += '\nmodified_sigma = "true"'
        branch = gmcm_branch_from_element_text(uncertainty)
        entry = registry.gmm_registry.get_by_identity(branch.registry_identity)
        rlz_gmm_map[branch_ids[idx][1:-1]] = entry
    return rlz_gmm_map


def build_rlz_source_map(source_lt: 'pandas.DataFrame') -> Dict[str, branch_registry.BranchRegistryEntry]:
    branch_ids = source_lt.index.tolist()
    rlz_source_map = dict()
    for idx, source_str in enumerate(source_lt.branch.tolist()):
        sources = "|".join(sorted(source_str.split('|')))
        entry = registry.source_registry.get_by_identity(sources)
        rlz_source_map[branch_ids[idx]] = entry
    return rlz_source_map


def build_rlz_map(rlz_lt: 'pandas.DataFrame', source_map: Dict, gmm_map: Dict) -> Dict[int, RealizationRecord]:
    paths = rlz_lt.branch_path.tolist()
    rlz_map = dict()
    for idx, path in enumerate(paths):
        src_key, gmm_key = path.split('~')
        rlz_map[idx] = RealizationRecord(idx=idx, path=path, sources=source_map[src_key], gmms=gmm_map[gmm_key])
    return rlz_map
