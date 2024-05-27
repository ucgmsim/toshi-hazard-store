import pytest

from toshi_hazard_store import query_v3


@pytest.fixture()
def build_realizations(adapted_rlz_model, build_rlzs_v3_models):
    with adapted_rlz_model.OpenquakeRealization.batch_write() as batch:
        for item in build_rlzs_v3_models():
            batch.save(item)


class TestQueryRlzsVs30:
    def test_query_rlzs_objects(self, adapted_rlz_model, build_realizations, many_rlz_args):
        qlocs = [loc.downsample(0.001).code for loc in many_rlz_args['locs']]
        print(f'qlocs {qlocs}')
        res = list(
            query_v3.get_rlz_curves_v3(
                locs=qlocs,
                vs30s=many_rlz_args['vs30s'],
                rlzs=many_rlz_args['rlzs'],
                tids=[many_rlz_args['TOSHI_ID']],
                imts=many_rlz_args['imts'],
            )
        )
        print(res)
        assert len(res) == len(many_rlz_args['rlzs']) * len(many_rlz_args['vs30s']) * len(many_rlz_args['locs'])
        assert res[0].nloc_001 == qlocs[0]

    @pytest.mark.parametrize(
        "vs30s",
        [[500, 1000], [1000], [1000, 1500], [500], [250, 500]],
        ids=['mixed', 'one_long', 'two_long', 'one_short', 'two_short'],
    )
    def test_query_hazard_aggr_with_vs30(self, adapted_rlz_model, build_realizations, many_rlz_args, vs30s):
        # vs30s = [500, 1000]
        qlocs = [loc.downsample(0.001).code for loc in many_rlz_args['locs']]
        res = list(
            query_v3.get_rlz_curves_v3(
                locs=qlocs,
                vs30s=vs30s,
                rlzs=many_rlz_args['rlzs'],
                tids=[many_rlz_args['TOSHI_ID']],
                imts=many_rlz_args['imts'],
            )
        )
        assert len(res) == len(many_rlz_args['rlzs']) * len(vs30s) * len(many_rlz_args['locs'])
        assert res[0].nloc_001 == qlocs[0]
        assert len(res[0].values[0].lvls) == 28
