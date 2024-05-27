# test_model_baseis_dynamic.p
from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model

from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter


class MyModel(Model):
    __metaclass__ = type

    class Meta:
        table_name = "MySQLITEModel"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)


class MySubclassedModel(MyModel):
    __metaclass__ = type

    class Meta:
        table_name = "MySQLITEModel"

    extra = UnicodeAttribute()


def test_default_class():
    instance = MyModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, MyModel)
    assert isinstance(instance, Model)
    # assert getattr(instance, 'exists') # interface method
    print(dir(instance))
    assert getattr(instance, 'my_hash_key')  # custom model attibute


def test_dynamic_baseclass_pynamodb():
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=Model,
    )
    instance = MyModel(my_hash_key='A', my_range_key='B')
    print(dir(instance))
    assert isinstance(instance, MyModel)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute


def test_dynamic_baseclass_sqlite():
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=SqliteAdapter,
    )
    instance = MyModel(my_hash_key='A2', my_range_key='B2')
    assert isinstance(instance, MyModel)
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute


def test_dynamic_baseclass_reassign():
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=Model,
    )

    instance = MyModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, MyModel)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute

    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=SqliteAdapter,
    )

    instance = MyModel(my_hash_key='A2', my_range_key='B2')

    assert isinstance(instance, MyModel)
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute


def test_dynamic_baseclass_reassign_reversed():

    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=SqliteAdapter,
    )

    instance = MyModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, Model)
    assert isinstance(instance, MyModel)

    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=Model,
    )

    instance = MyModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, MyModel)
    assert isinstance(instance, Model)
    assert not isinstance(instance, SqliteAdapter)


def test_default_subclass():
    instance = MySubclassedModel(my_hash_key='A', my_range_key='B', extra="C")
    assert isinstance(instance, MySubclassedModel)
    assert isinstance(instance, Model)
    # assert getattr(instance, 'exists') # interface method
    print(dir(instance))
    assert getattr(instance, 'my_hash_key')  # custom model attibute


def test_dynamic_subclass_pynamodb():
    # we reassign the base class where Model is uses
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=Model,
    )
    instance = MySubclassedModel(my_hash_key='A', my_range_key='B', extra="C")
    print(dir(instance))
    assert isinstance(instance, MySubclassedModel)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute
    assert getattr(instance, 'extra')  # custom model attibute


def test_dynamic_subclass_sqlite():
    # we reassign the base class where Model is uses
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=SqliteAdapter,
    )
    instance = MySubclassedModel(my_hash_key='A', my_range_key='B', extra="C")
    print(dir(instance))
    assert isinstance(instance, MySubclassedModel)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute
    assert getattr(instance, 'extra')  # custom model attibute


def test_dynamic_subclass_reassign():
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=Model,
    )

    instance = MySubclassedModel(my_hash_key='A', my_range_key='B', extra="C")
    print(dir(instance))
    assert isinstance(instance, MySubclassedModel)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute
    assert getattr(instance, 'extra')  # custom model attibute

    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=SqliteAdapter,
    )

    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySubclassedModel'),
        base_class=MyModel,
    )

    instance = MySubclassedModel(my_hash_key='A1', my_range_key='B1', extra="C1")
    print(dir(instance))
    print('bases', MySubclassedModel.__bases__)

    assert isinstance(instance, MySubclassedModel)
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # baseclass attibute
    assert getattr(instance, 'extra')  # subclass attibute


def test_dynamic_subclass_reassign_reversed():

    # Configure for SQLIte adapter
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=SqliteAdapter,
    )
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySubclassedModel'),
        base_class=MyModel,
    )

    instance = MySubclassedModel(my_hash_key='A1', my_range_key='B1', extra="C1")

    assert isinstance(instance, MySubclassedModel)
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, MyModel)
    assert isinstance(instance, Model)

    # reconfigure for native Pynamodb Model
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MyModel'),
        base_class=Model,
    )
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySubclassedModel'),
        base_class=MyModel,
    )

    instance = MySubclassedModel(my_hash_key='A', my_range_key='B', extra="C")

    assert isinstance(instance, MySubclassedModel)
    assert isinstance(instance, Model)
    assert isinstance(instance, MyModel)
    assert not isinstance(instance, SqliteAdapter)
