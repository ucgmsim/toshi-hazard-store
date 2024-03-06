import logging
import os
from unittest import mock

import pytest
from moto import mock_dynamodb

# from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model


from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter

from toshi_hazard_store.model.revision_4 import hazard_models  # the module containing adaptable model(s)

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
