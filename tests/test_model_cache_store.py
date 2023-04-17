from toshi_hazard_store import model
from toshi_hazard_store.model.caching import cache_store


class TestCacheStoreSQLExpressions:
    def test_range_key_expression(self):
        condition = model.HazardAggregation.sort_key >= '-43.200~177.270:700:PGA'
        print(condition)
        print('operator', condition.operator)
        assert next(cache_store.sql_from_pynamodb_condition(condition)) == "sort_key >= \"-43.200~177.270:700:PGA\""

    def test_filter_condition_unary_eq_number(self):
        mHAG = model.HazardAggregation
        condition = mHAG.vs30 == 700
        assert next(cache_store.sql_from_pynamodb_condition(condition)) == "vs30 = 700"

    def test_filter_condition_unary_gt_number(self):
        mHAG = model.HazardAggregation
        condition = mHAG.vs30 > 700
        assert next(cache_store.sql_from_pynamodb_condition(condition)) == "vs30 > 700"

    def test_filter_condition_unary_lt_number(self):
        mHAG = model.HazardAggregation
        condition = mHAG.vs30 < 700
        assert next(cache_store.sql_from_pynamodb_condition(condition)) == "vs30 < 700"

    def test_filter_condition_unary_eq_string(self):
        mHAG = model.HazardAggregation
        condition = mHAG.imt == "PGA"
        assert next(cache_store.sql_from_pynamodb_condition(condition)) == "imt = \"PGA\""

    def test_filter_condition_in_number_list(self):
        mHAG = model.HazardAggregation
        condition = mHAG.vs30.is_in(*[700, 800])
        assert (
            next(cache_store.sql_from_pynamodb_condition(condition)) == 'vs30 IN (700, 800)'
        )  # https://www.dofactory.com/sql/where-in

    def test_filter_condition_in_string_list(self):
        mHAG = model.HazardAggregation
        condition = mHAG.imt.is_in(*["SA(0.5)"])
        assert (
            next(cache_store.sql_from_pynamodb_condition(condition)) == 'imt IN ("SA(0.5)")'
        )  # https://www.dofactory.com/sql/where-in

    def test_filter_condition_two(self):
        mHAG = model.HazardAggregation
        condition = mHAG.vs30.is_in(*[700, 800, 350]) & mHAG.imt.is_in(*['PGA', 'SA(0.5)'])
        print(condition)
        assert list(cache_store.sql_from_pynamodb_condition(condition)) == [
            'vs30 IN (700, 800, 350)',
            'imt IN ("PGA", "SA(0.5)")',
        ]

    def test_filter_condition_three(self):
        mHAG = model.HazardAggregation
        condition = (
            mHAG.vs30.is_in(*[250, 350])
            & mHAG.imt.is_in(*['PGA', 'SA(0.5)'])
            & mHAG.hazard_model_id.is_in('MODEL_THE_FIRST')
        )
        print(condition)
        assert list(cache_store.sql_from_pynamodb_condition(condition)) == [
            'vs30 IN (250, 350)',
            'imt IN ("PGA", "SA(0.5)")',
            'hazard_model_id IN ("MODEL_THE_FIRST")',
        ]


class TestPermutationCountsFromExpressions:
    def test_filter_condition_count_four(self):
        mHAG = model.HazardAggregation
        condition = (
            mHAG.vs30.is_in(*[250, 350])
            & mHAG.imt.is_in(*['PGA', 'SA(0.5)'])
            & mHAG.hazard_model_id.is_in('MODEL_THE_FIRST')
        )
        print(condition)
        assert cache_store.count_permutations(condition) == 4

    def test_filter_condition_count_eight(self):
        mHAG = model.HazardAggregation
        condition = (
            mHAG.vs30.is_in(*[250, 350])
            & mHAG.imt.is_in(*['PGA', 'SA(0.5)'])
            & mHAG.hazard_model_id.is_in('MODEL_THE_FIRST', "B")
        )
        print(condition)
        assert cache_store.count_permutations(condition) == 8

    def test_filter_condition_count_27(self):
        mHAG = model.HazardAggregation
        condition = (
            mHAG.vs30.is_in(*[250, 350, 450])
            & mHAG.imt.is_in(*['PGA', 'SA(0.5)', 'SA(1.0)'])
            & mHAG.hazard_model_id.is_in('MODEL_THE_FIRST', "B", "C")
        )
        print(condition)
        assert cache_store.count_permutations(condition) == 27
