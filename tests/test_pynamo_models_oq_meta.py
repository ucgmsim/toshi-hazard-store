# TODO: these were the first adapter tests implemented, and now this is done in conftest.py - consider porting these.
import os
from unittest import mock

import pytest
from moto import mock_dynamodb
from pynamodb.models import Model

from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter
from toshi_hazard_store.model import openquake_models


def set_adapter(adapter):
    print(dir())
    ensure_class_bases_begin_with(
        namespace=openquake_models.__dict__,
        class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
        base_class=adapter,
    )


@pytest.fixture
def adapted_model(request, tmp_path):
    if request.param == 'pynamodb':
        with mock_dynamodb():
            openquake_models.ToshiOpenquakeMeta.create_table(wait=True)
            # openquake_models.OpenquakeRealization.create_table(wait=True)
            yield openquake_models
            openquake_models.ToshiOpenquakeMeta.delete_table()
            # openquake_models.OpenquakeRealization.delete_table()
    elif request.param == 'sqlite':
        envvars = {"THS_SQLITE_FOLDER": str(tmp_path), "THS_USE_SQLITE_ADAPTER": "TRUE"}
        with mock.patch.dict(os.environ, envvars, clear=True):
            set_adapter(SqliteAdapter)
            openquake_models.ToshiOpenquakeMeta.create_table(wait=True)
            yield openquake_models
            openquake_models.ToshiOpenquakeMeta.delete_table()
            # openquake_models.OpenquakeRealization.delete_table()
    else:
        raise ValueError("invalid internal test config")


# ref https://docs.pytest.org/en/7.3.x/example/parametrize.html#deferring-the-setup-of-parametrized-resources
def pytest_generate_tests(metafunc):
    if "adapted_model" in metafunc.fixturenames:
        metafunc.parametrize("adapted_model", ["pynamodb", "sqlite"], indirect=True)


class TestToshiOpenquakeMetaModel:
    def test_table_exists(self, adapted_model):
        # assert adapted_model.OpenquakeRealization.exists()
        assert adapted_model.ToshiOpenquakeMeta.exists()

    def test_save_one_meta_object(self, adapted_model, get_one_meta):
        print(openquake_models.__dict__['ToshiOpenquakeMeta'].__bases__)
        with mock_dynamodb():
            # model.ToshiOpenquakeMeta.create_table(wait=True)
            obj = get_one_meta(openquake_models.ToshiOpenquakeMeta)
            obj.save()
            assert obj.inv_time == 1.0
        # assert adapted_model == 2

    def test_dynamic_baseclass_adapter_sqlite(self, get_one_meta):
        ensure_class_bases_begin_with(
            namespace=openquake_models.__dict__,
            class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
            base_class=SqliteAdapter,
        )

        instance = get_one_meta()
        assert isinstance(instance, SqliteAdapter)
        assert isinstance(instance, Model)
        assert getattr(instance, 'exists')  # interface method
        assert getattr(instance, 'partition_key')  # model attribute

    @pytest.mark.skip('fiddle')
    def test_default_baseclass_adapter_pynamodb(self, get_one_meta):
        #   assert not isinstance(MySqlModel(my_hash_key='A', my_range_key='B'), Model)
        # print(model.__dict__['ToshiOpenquakeMeta'])
        # print(model.__dict__['ToshiOpenquakeMeta'].__bases__)
        ensure_class_bases_begin_with(
            namespace=openquake_models.__dict__,
            class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
            base_class=Model,
        )
        print(openquake_models.__dict__['ToshiOpenquakeMeta'].__bases__)

        instance = get_one_meta()

        print(openquake_models.ToshiOpenquakeMeta.__bases__)
        assert not isinstance(instance, SqliteAdapter)
        assert isinstance(instance, Model)
        assert getattr(instance, 'exists')  # interface method
        assert getattr(instance, 'partition_key')  # model attribute
