import itertools
import logging
import os
from unittest import mock

import pytest
from moto import mock_dynamodb
from nzshm_common.location.coded_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID
from pynamodb.models import Model

from toshi_hazard_store import model  # noqa
from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter
from toshi_hazard_store.model.revision_4 import hazard_models  # noqa
from toshi_hazard_store.model.revision_4 import hazard_aggregate_curve, hazard_realization_curve

log = logging.getLogger(__name__)


# ref https://docs.pytest.org/en/7.3.x/example/parametrize.html#deferring-the-setup-of-parametrized-resources
def pytest_generate_tests(metafunc):
    if "adapted_model" in metafunc.fixturenames:
        metafunc.parametrize("adapted_model", ["pynamodb", "sqlite"], indirect=True)


@pytest.fixture
def adapted_model(request, tmp_path):
    """This fixture reconfigures adaption of all table in the hazard_models module"""
    models = itertools.chain(
        hazard_models.get_tables(), hazard_realization_curve.get_tables(), hazard_aggregate_curve.get_tables()
    )

    class AdaptedModelFixture:
        HazardRealizationCurve = None
        HazardCurveProducerConfig = None
        CompatibleHazardCalculation = None
        HazardAggregateCurve = None

    def set_adapter(model_klass, adapter):
        print(f'*** setting {model_klass.__name__} to adapter {adapter}')
        if model_klass.__name__ == 'HazardAggregateCurve':
            ensure_class_bases_begin_with(
                namespace=hazard_aggregate_curve.__dict__,
                class_name=model_klass.__name__,  # `str` type differs on Python 2 vs. 3.
                base_class=adapter,
            )
        elif model_klass.__name__ == 'HazardRealizationCurve':
            ensure_class_bases_begin_with(
                namespace=hazard_realization_curve.__dict__,
                class_name=model_klass.__name__,  # `str` type differs on Python 2 vs. 3.
                base_class=adapter,
            )
        else:
            ensure_class_bases_begin_with(
                namespace=hazard_models.__dict__,
                class_name=model_klass.__name__,  # `str` type differs on Python 2 vs. 3.
                base_class=adapter,
            )

    def new_model_fixture():
        model_fixture = AdaptedModelFixture()
        model_fixture.HazardRealizationCurve = globals()['hazard_realization_curve'].HazardRealizationCurve
        model_fixture.HazardCurveProducerConfig = globals()['hazard_models'].HazardCurveProducerConfig
        model_fixture.CompatibleHazardCalculation = globals()['hazard_models'].CompatibleHazardCalculation
        model_fixture.HazardAggregateCurve = globals()['hazard_aggregate_curve'].HazardAggregateCurve
        return model_fixture

    def migrate_models():
        hazard_models.migrate()
        hazard_realization_curve.migrate()
        hazard_aggregate_curve.migrate()

    def drop_models():
        hazard_models.drop_tables()
        hazard_realization_curve.drop_tables()
        hazard_aggregate_curve.drop_tables()

    if request.param == 'pynamodb':
        with mock_dynamodb():
            for model_klass in models:
                set_adapter(model_klass, Model)

            migrate_models()
            yield new_model_fixture()
            drop_models()

    elif request.param == 'sqlite':
        envvars = {"THS_SQLITE_FOLDER": str(tmp_path), "THS_USE_SQLITE_ADAPTER": "TRUE"}
        with mock.patch.dict(os.environ, envvars, clear=True):
            for model_klass in models:
                set_adapter(model_klass, SqliteAdapter)
            migrate_models()
            yield new_model_fixture()
            drop_models()

    else:
        raise ValueError("invalid internal test config")


@pytest.fixture
def many_rlz_args():
    yield dict(
        # TOSHI_ID='FAk3T0sHi1D==',
        vs30s=[250, 1500],
        imts=['PGA', 'SA(0.5)'],
        locs=[CodedLocation(o['latitude'], o['longitude'], 0.001) for o in list(LOCATIONS_BY_ID.values())[-5:]],
        sources=["c9d8be924ee7"],
        gmms=["a7d8c5d537e1"],
    )


@pytest.fixture(scope='function')
def generate_rev4_rlz_models(many_rlz_args, adapted_model):
    def model_generator():
        # values = list(map(lambda x: LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))
        values = list(map(lambda x: x / 1e6, range(1, 51)))
        for loc, vs30, imt, source, gmm in itertools.product(
            many_rlz_args["locs"][:5],
            many_rlz_args["vs30s"],
            many_rlz_args["imts"],
            many_rlz_args["sources"],
            many_rlz_args["gmms"],
        ):
            yield hazard_realization_curve.HazardRealizationCurve(
                compatible_calc_fk=("A", "AA"),
                producer_config_fk=("B", "BB"),
                values=values,
                imt=imt,
                vs30=vs30,
                sources_digest=source,
                gmms_digest=gmm,
                # site_vs30=vs30,
                # hazard_solution_id=many_rlz_args["TOSHI_ID"],
                # source_tags=['TagOne'],
                # source_ids=['Z', 'XX'],
            ).set_location(loc)

    yield model_generator
