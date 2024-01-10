import os
import unittest
from unittest import mock

import pynamodb.exceptions
import pytest
from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation
from pynamodb.models import Model

import toshi_hazard_store
from toshi_hazard_store import model
from toshi_hazard_store.v2.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.v2.db_adapter.sqlite import SqliteAdapter


def set_adapter(adapter):
    print(dir())
    ensure_class_bases_begin_with(
        namespace=model.__dict__,
        class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
        base_class=adapter,
    )


@pytest.fixture
def adapted_model(request, tmp_path):
    if request.param == 'pynamodb':
        # for table_name in ['ToshiOpenquakeMeta']:
        #     ensure_class_bases_begin_with(
        #         namespace=model.__dict__,
        #         class_name=table_name,  # `str` type differs on Python 2 vs. 3.
        #         base_class=Model,
        #     )
        with mock_dynamodb():
            model.ToshiOpenquakeMeta.create_table(wait=True)
            # model.OpenquakeRealization.create_table(wait=True)
            yield model
            model.ToshiOpenquakeMeta.delete_table()
            # model.OpenquakeRealization.delete_table()
    elif request.param == 'sqlite':
        envvars = {"THS_SQLITE_FOLDER": str(tmp_path), "THS_USE_SQLITE_ADAPTER": "TRUE"}
        with mock.patch.dict(os.environ, envvars, clear=True):
            set_adapter(SqliteAdapter)
            model.ToshiOpenquakeMeta.create_table(wait=True)
            yield model
            model.ToshiOpenquakeMeta.delete_table()
            # model.OpenquakeRealization.delete_table()
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

    def test_save_one_meta_object(self, get_one_meta, adapted_model):
        print(model.__dict__['ToshiOpenquakeMeta'].__bases__)
        with mock_dynamodb():
            # model.ToshiOpenquakeMeta.create_table(wait=True)
            obj = get_one_meta()
            obj.save()
            assert obj.inv_time == 1.0
        # assert adapted_model == 2

    def test_dynamic_baseclass_adapter_sqlite(self, get_one_meta):
        ensure_class_bases_begin_with(
            namespace=toshi_hazard_store.model.__dict__,
            class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
            base_class=SqliteAdapter,
        )

        instance = get_one_meta()
        assert isinstance(instance, SqliteAdapter)
        assert isinstance(instance, Model)
        assert getattr(instance, 'exists')  # interface method
        assert getattr(instance, 'partition_key')  # model attribute

    def test_default_baseclass_adapter_pynamodb(self, get_one_meta):
        #   assert not isinstance(MySqlModel(my_hash_key='A', my_range_key='B'), Model)
        # print(model.__dict__['ToshiOpenquakeMeta'])
        # print(model.__dict__['ToshiOpenquakeMeta'].__bases__)
        ensure_class_bases_begin_with(
            namespace=toshi_hazard_store.model.__dict__,
            class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
            base_class=Model,
        )
        print(model.__dict__['ToshiOpenquakeMeta'].__bases__)

        instance = get_one_meta()

        print(model.ToshiOpenquakeMeta.__bases__)
        assert not isinstance(instance, SqliteAdapter)
        assert isinstance(instance, Model)
        assert getattr(instance, 'exists')  # interface method
        assert getattr(instance, 'partition_key')  # model attribute
