import logging
import os
from unittest import mock

import pytest
import itertools
from moto import mock_dynamodb

# from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model

from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID

from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter

from toshi_hazard_store.model.revision_4 import hazard_models  # the module containing adaptable model(s)

from toshi_hazard_store.model.attributes import IMTValuesAttribute


log = logging.getLogger(__name__)

# cache_folder = tempfile.TemporaryDirectory()
# adapter_folder = tempfile.TemporaryDirectory()


# ref https://docs.pytest.org/en/7.3.x/example/parametrize.html#deferring-the-setup-of-parametrized-resources
def pytest_generate_tests(metafunc):
    if "adapted_model" in metafunc.fixturenames:
        metafunc.parametrize("adapted_model", ["pynamodb", "sqlite"], indirect=True)


@pytest.fixture
def adapted_model(request, tmp_path):
    """This fixture reconfigures adaption of all table in the hazard_models module"""
    models = hazard_models.get_tables()

    def set_adapter(model_klass, adapter):
        ensure_class_bases_begin_with(
            namespace=hazard_models.__dict__,
            class_name=model_klass.__name__,  # `str` type differs on Python 2 vs. 3.
            base_class=adapter,
        )

    if request.param == 'pynamodb':
        with mock_dynamodb():
            for model_klass in models:
                set_adapter(model_klass, Model)
            hazard_models.migrate()
            yield hazard_models
            hazard_models.drop_tables()

    elif request.param == 'sqlite':
        envvars = {"THS_SQLITE_FOLDER": str(tmp_path), "THS_USE_SQLITE_ADAPTER": "TRUE"}
        with mock.patch.dict(os.environ, envvars, clear=True):
            for model_klass in models:
                set_adapter(model_klass, SqliteAdapter)
            hazard_models.migrate()
            yield hazard_models
            hazard_models.drop_tables()
    else:
        raise ValueError("invalid internal test config")



@pytest.fixture
def many_rlz_args():
    yield dict(
        # TOSHI_ID='FAk3T0sHi1D==',
        vs30s=[250, 500, 1000, 1500],
        imts=['PGA', 'SA(0.5)'],
        locs=[CodedLocation(o['latitude'], o['longitude'], 0.001) for o in list(LOCATIONS_BY_ID.values())[-5:]],
        rlzs=[str(x) for x in range(5)],
    )

@pytest.fixture(scope='function')
def generate_rev4_rlz_models(many_rlz_args, adapted_model):

    n_lvls = 29

    def model_generator():
        for rlz in many_rlz_args['rlzs']:
            values = []
            for imt, val in enumerate(many_rlz_args['imts']):
                values.append(
                    IMTValuesAttribute(
                        imt=val,
                        lvls=[x / 1e3 for x in range(1, n_lvls)],
                        vals=[x / 1e6 for x in range(1, n_lvls)],
                    )
                )
            for loc, vs30 in itertools.product(many_rlz_args["locs"][:5], many_rlz_args["vs30s"]):
                yield hazard_models.HazardRealizationCurve(
                    compatible_calc_fk="A",
                    producer_config_fk="BBB",
                    values=values,
                    rlz=rlz,
                    vs30=vs30,
                    # site_vs30=vs30,
                    # hazard_solution_id=many_rlz_args["TOSHI_ID"],
                    # source_tags=['TagOne'],
                    # source_ids=['Z', 'XX'],
                ).set_location(loc)

    yield model_generator