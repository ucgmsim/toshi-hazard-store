# test_model_baseis_dynamic.p
from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model

import toshi_hazard_store
from toshi_hazard_store import model
from toshi_hazard_store.v2.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.v2.db_adapter.sqlite import SqliteAdapter


class MySqlModel(Model):
    __metaclass__ = type

    class Meta:
        table_name = "MySQLITEModel"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)


def test_basic_class():
    instance = MySqlModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, MySqlModel)
    assert isinstance(instance, Model)
    # assert getattr(instance, 'exists') # interface method
    print(dir(instance))
    assert getattr(instance, 'my_hash_key')  # custom model attibute


def test_dynamic_baseclass_pynamodb():
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySqlModel'),  # `str` type differs on Python 2 vs. 3.
        base_class=Model,
    )
    instance = MySqlModel(my_hash_key='A', my_range_key='B')
    print(dir(instance))
    assert isinstance(instance, MySqlModel)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute


def test_dynamic_baseclass_sqlite():
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySqlModel'),  # `str` type differs on Python 2 vs. 3.
        base_class=SqliteAdapter,
    )
    instance = MySqlModel(my_hash_key='A2', my_range_key='B2')
    assert isinstance(instance, MySqlModel)
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute


def test_dynamic_baseclass():
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySqlModel'),  # `str` type differs on Python 2 vs. 3.
        base_class=Model,
    )

    instance = MySqlModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, MySqlModel)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute

    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySqlModel'),  # `str` type differs on Python 2 vs. 3.
        base_class=SqliteAdapter,
    )

    instance = MySqlModel(my_hash_key='A2', my_range_key='B2')

    assert isinstance(instance, MySqlModel)
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute
