"""Helpers for querying Hazard Realizations and related models  - Revision 4.

Provides efficient queries for the models: **HazardRealizationCurve*.*

Functions:

 - **get_rlz_curves)**   - returns iterator of matching OpenquakeRealization objects.

"""

import decimal
import itertools
import logging
from typing import Iterable, Iterator

from nzshm_common.location.code_location import CodedLocation

from toshi_hazard_store.model.revision_4 import hazard_models

log = logging.getLogger(__name__)


def downsample_code(loc_code, res):
    lt = loc_code.split('~')
    assert len(lt) == 2
    return CodedLocation(lat=float(lt[0]), lon=float(lt[1]), resolution=res).code


def get_hashes(locs: Iterable[str]):
    hashes = set()
    for loc in locs:
        lt = loc.split('~')
        assert len(lt) == 2
        hashes.add(downsample_code(loc, 0.1))
    return sorted(list(hashes))


def get_rlz_curves(
    locs: Iterable[str],
    vs30s: Iterable[int],
    imts: Iterable[str],
) -> Iterator[hazard_models.HazardRealizationCurve]:
    """Query the HazardRealizationCurve table.

    Parameters:
        locs: coded location codes e.g. ['-46.430~168.360']
        vs30s: vs30 values eg [400, 500]
        imts: imt (IntensityMeasureType) values e.g ['PGA', 'SA(0.5)']

    Yields:
        HazardRealizationCurve models
    """

    # table classes may be rebased, this makes sure we always get the current class definition.
    mRLZ = hazard_models.__dict__['HazardRealizationCurve']

    def build_condition_expr(loc, vs30, imt):
        """Build the filter condition expression."""
        grid_res = decimal.Decimal(str(loc.split('~')[0]))
        places = grid_res.as_tuple().exponent

        res = float(decimal.Decimal(10) ** places)
        loc = downsample_code(loc, res)
        expr = None

        if places == -1:
            expr = mRLZ.nloc_1 == loc
        elif places == -2:
            expr = mRLZ.nloc_01 == loc
        elif places == -3:
            expr = mRLZ.nloc_001 == loc
        else:
            assert 0
        return expr & (mRLZ.vs30 == vs30) & (mRLZ.imt == imt)

    total_hits = 0
    for hash_location_code in get_hashes(locs):
        partition_hits = 0
        log.debug('hash_key %s' % hash_location_code)
        hash_locs = list(filter(lambda loc: downsample_code(loc, 0.1) == hash_location_code, locs))

        for hloc, vs30, imt in itertools.product(hash_locs, vs30s, imts):

            sort_key_first_val = f"{hloc}:{str(vs30).zfill(4)}:{imt}"
            condition_expr = build_condition_expr(hloc, vs30, imt)

            log.debug('sort_key_first_val: %s' % sort_key_first_val)
            log.debug('condition_expr: %s' % condition_expr)

            results = mRLZ.query(
                hash_location_code,
                mRLZ.sort_key >= sort_key_first_val,
                filter_condition=condition_expr,
            )

            # print(f"get_hazard_rlz_curves_v3: qry {qry}")
            log.debug("get_hazard_rlz_curves_v3: results %s" % results)
            for hit in results:
                partition_hits += 1
                # hit.values = list(filter(lambda x: x.imt in imts, hit.values))
                yield (hit)

        total_hits += partition_hits
        log.debug('hash_key %s has %s hits' % (hash_location_code, partition_hits))

    log.info('Total %s hits' % total_hits)


##
# DEMO code below, to migrate to tests and/or docs
##

if __name__ == '__main__':

    logging.basicConfig(level=logging.ERROR)
    from nzshm_common.location.location import LOCATIONS_BY_ID

    locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in list(LOCATIONS_BY_ID.values())[:1]]

    for res in get_rlz_curves([loc.code for loc in locs], [400], ['PGA', 'SA(1.0)']):
        print(
            [res.nloc_001, res.vs30, res.imt, res.source_branch, res.gmm_branch, res.compatible_calc_fk, res.values[:4]]
        )

    def parse_lts():

        import pathlib

        from openquake.calculators.extract import Extractor

        from toshi_hazard_store.transform import parse_logic_tree_branches

        hdf5 = pathlib.Path(
            "./WORKING/",
            "R2VuZXJhbFRhc2s6MTMyODQxNA==",
            "subtasks",
            "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3",
            "calc_1.hdf5",
        )
        assert hdf5.exists()

        extractor = Extractor(str(hdf5))
        # rlzs = extractor.get('hcurves?kind=rlzs', asdict=True)
        # rlz_keys = [k for k in rlzs.keys() if 'rlz-' in k]

        source_lt, gsim_lt, rlz_lt = parse_logic_tree_branches(extractor)
        print("GSIMs")
        print(gsim_lt)

        print()
        print("Sources")
        print(source_lt)
        print()
        print(source_lt["branch"].tolist()[0].split('|'))

        print()
        print("RLZs")
        print(rlz_lt)

    # play with LTS
    print()
    parse_lts()
