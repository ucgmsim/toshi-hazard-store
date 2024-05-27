from toshi_hazard_store import query_v3

# HAZARD_MODEL_ID = 'MODEL_THE_FIRST'
# vs30s = [250, 350, 450]
# imts = ['PGA', 'SA(0.5)']
# aggs = [model.AggregationEnum.MEAN.value, model.AggregationEnum._10.value]
# locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in LOCATIONS_BY_ID.values()]


class TestQueryHazardAggregationV3:
    def test_query_hazard_aggr(self, build_hazagg_models, adapted_hazagg_model, many_hazagg_args):

        qlocs = [loc.downsample(0.001).code for loc in many_hazagg_args['locs'][:2]]
        print(f'qlocs {qlocs}')
        res = list(
            query_v3.get_hazard_curves(
                locs=qlocs,
                vs30s=many_hazagg_args['vs30s'],
                hazard_model_ids=[many_hazagg_args['HAZARD_MODEL_ID']],
                imts=many_hazagg_args['imts'],
            )
        )
        print(res)
        assert len(res) == len(many_hazagg_args['imts']) * len(many_hazagg_args['aggs']) * len(
            many_hazagg_args['vs30s']
        ) * len(qlocs)
        assert res[0].nloc_001 == qlocs[0]

    def test_query_hazard_aggr_2(self, build_hazagg_models, adapted_hazagg_model, many_hazagg_args):
        qlocs = [loc.downsample(0.001).code for loc in many_hazagg_args['locs'][:2]]
        res = list(
            query_v3.get_hazard_curves(
                # qlocs, vs30s, [HAZARD_MODEL_ID, 'FAKE_ID'], imts)
                locs=qlocs,
                vs30s=many_hazagg_args['vs30s'],
                hazard_model_ids=[many_hazagg_args['HAZARD_MODEL_ID'], 'FAKE_ID'],
                imts=many_hazagg_args['imts'],
            )
        )
        assert len(res) == len(many_hazagg_args['imts']) * len(many_hazagg_args['aggs']) * len(
            many_hazagg_args['vs30s']
        ) * len(qlocs)
        assert res[0].nloc_001 == qlocs[0]

    def test_query_hazard_aggr_single(self, build_hazagg_models, adapted_hazagg_model, many_hazagg_args):
        qlocs = [loc.downsample(0.001).code for loc in many_hazagg_args['locs'][:2]]
        print(f'qlocs {qlocs}')
        res = list(
            query_v3.get_hazard_curves(
                locs=qlocs[:1],
                vs30s=many_hazagg_args['vs30s'][:1],
                hazard_model_ids=[many_hazagg_args['HAZARD_MODEL_ID']],
                imts=many_hazagg_args['imts'][:1],
                aggs=['mean'],
            )
        )
        print(res)
        assert len(res) == 1
        assert res[0].nloc_001 == qlocs[0]
