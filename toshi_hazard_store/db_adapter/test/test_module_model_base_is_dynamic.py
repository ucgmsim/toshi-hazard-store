import importlib
import sys

from pynamodb.models import Model

from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter

from . import module_model_rebase_fixtures


def test_dynamic_subclass_reassign():

    importlib.reload(sys.modules['toshi_hazard_store.db_adapter.test.module_model_rebase_fixtures'])

    module_namespace = module_model_rebase_fixtures.__dict__
    ensure_class_bases_begin_with(
        namespace=module_namespace,
        class_name=str('MyModel'),
        base_class=Model,
    )
    ensure_class_bases_begin_with(
        namespace=module_namespace,
        class_name=str('MySubclassedModel'),
        base_class=module_model_rebase_fixtures.MyModel,
    )

    instance = module_model_rebase_fixtures.MySubclassedModel(my_hash_key='A', my_range_key='B', extra="C")
    print(dir(instance))
    assert isinstance(instance, module_model_rebase_fixtures.MySubclassedModel)
    assert isinstance(instance, module_model_rebase_fixtures.MyModel)
    assert isinstance(instance, Model)
    assert not isinstance(instance, SqliteAdapter)

    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute
    assert getattr(instance, 'extra')  # custom model attibute

    ensure_class_bases_begin_with(
        namespace=module_namespace,
        class_name=str('MyModel'),
        base_class=SqliteAdapter,
    )

    ensure_class_bases_begin_with(
        namespace=module_namespace,
        class_name=str('MySubclassedModel'),
        base_class=module_model_rebase_fixtures.MyModel,
    )

    instance = module_model_rebase_fixtures.MySubclassedModel(my_hash_key='A1', my_range_key='B1', extra="C1")
    print(dir(instance))
    print('bases', module_model_rebase_fixtures.MySubclassedModel.__bases__)

    assert isinstance(instance, module_model_rebase_fixtures.MySubclassedModel)
    assert isinstance(instance, module_model_rebase_fixtures.MyModel)
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, Model)
    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # baseclass attibute
    assert getattr(instance, 'extra')  # subclass attibute


def test_dynamic_subclass_reassign_reversed():

    # importlib.reload(sys.modules['toshi_hazard_store.db_adapter.test.module_model_rebase_fixtures'])

    module_namespace = module_model_rebase_fixtures.__dict__

    ensure_class_bases_begin_with(
        namespace=module_namespace,
        class_name=str('MyModel'),
        base_class=SqliteAdapter,
    )

    ensure_class_bases_begin_with(
        namespace=module_namespace,
        class_name=str('MySubclassedModel'),
        base_class=module_model_rebase_fixtures.MyModel,
    )

    instance = module_model_rebase_fixtures.MySubclassedModel(my_hash_key='A1', my_range_key='B1', extra="C1")

    print('MySubclassedModel bases', module_model_rebase_fixtures.MySubclassedModel.__bases__)
    print('MyModel bases', module_model_rebase_fixtures.MyModel.__bases__)

    assert isinstance(instance, module_model_rebase_fixtures.MySubclassedModel)
    assert isinstance(instance, module_model_rebase_fixtures.MyModel)
    assert isinstance(instance, SqliteAdapter)
    assert isinstance(instance, Model)

    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # baseclass attibute
    assert getattr(instance, 'extra')  # subclass attibute

    ensure_class_bases_begin_with(
        namespace=module_namespace,
        class_name=str('MyModel'),
        base_class=Model,
    )
    ensure_class_bases_begin_with(
        namespace=module_namespace,
        class_name=str('MySubclassedModel'),
        base_class=module_model_rebase_fixtures.MyModel,
    )

    instance = module_model_rebase_fixtures.MySubclassedModel(my_hash_key='A', my_range_key='B', extra="C")

    print('MySubclassedModel bases', module_model_rebase_fixtures.MySubclassedModel.__bases__)
    print('MyModel bases', module_model_rebase_fixtures.MyModel.__bases__)

    assert isinstance(instance, module_model_rebase_fixtures.MySubclassedModel)
    assert isinstance(instance, module_model_rebase_fixtures.MyModel)
    assert isinstance(instance, Model)
    assert not isinstance(instance, SqliteAdapter)

    assert getattr(instance, 'exists')  # interface method
    assert getattr(instance, 'my_hash_key')  # custom model attibute
    assert getattr(instance, 'extra')  # custom model attibute
