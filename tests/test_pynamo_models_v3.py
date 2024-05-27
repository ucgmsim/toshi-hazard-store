import pytest

# # ref https://docs.pytest.org/en/7.3.x/example/parametrize.html#deferring-the-setup-of-parametrized-resources
# def pytest_generate_tests(metafunc):
#     if "adapted_hazagg_model" in metafunc.fixturenames:
#         metafunc.parametrize("adapted_hazagg_model", ["pynamodb", "sqlite"], indirect=True)


class TestHazardAggregationModel:
    def test_table_exists(self, adapted_hazagg_model):
        assert adapted_hazagg_model.HazardAggregation.exists()

    def test_save_one_new_hazard_object(self, adapted_hazagg_model, get_one_hazagg):
        """New realization handles all the IMT levels."""
        print(adapted_hazagg_model.__dict__['HazardAggregation'].__bases__)

        hazagg = get_one_hazagg()
        # print(f'hazagg: {hazagg} {hazagg.version}')
        hazagg.save()
        # print(f'hazagg: {hazagg} {hazagg.version}')
        # print(dir(hazagg))
        assert hazagg.values[0].lvl == 0.001
        assert hazagg.values[0].val == 1e-6
        assert hazagg.partition_key == '-41.3~174.8'  # 0.1 degree res


class TestHazardAggregationQuery:
    def test_model_query_no_condition(self, adapted_hazagg_model, get_one_hazagg):
        hag = get_one_hazagg()
        hag.save()

        # query on model without range_key is not allowed
        with pytest.raises(TypeError):
            list(adapted_hazagg_model.HazardAggregation.query(hag.partition_key))[0]
        # self.assertEqual(res.partition_key, hag.partition_key)
        # self.assertEqual(res.sort_key, hag.sort_key)

    def test_model_query_equal_condition(self, adapted_hazagg_model, get_one_hazagg):
        hag = get_one_hazagg()
        hag.save()

        mHAG = adapted_hazagg_model.HazardAggregation
        range_condition = mHAG.sort_key == '-41.300~174.780:450:PGA:mean:HAZ_MODEL_ONE'
        filter_condition = mHAG.vs30.is_in(450) & mHAG.imt.is_in('PGA') & mHAG.hazard_model_id.is_in('HAZ_MODEL_ONE')

        # query on model
        res = list(
            adapted_hazagg_model.HazardAggregation.query(
                hag.partition_key,
                range_condition,
                filter_condition,
                # model.HazardAggregation.sort_key == '-41.300~174.780:450:PGA:mean:HAZ_MODEL_ONE'
            )
        )[0]
        assert res.partition_key == hag.partition_key
        assert res.sort_key == hag.sort_key
