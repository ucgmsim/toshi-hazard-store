import io
import pickle

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


def test_pickle_pyanmodb_model():

    obj = MyModel(my_hash_key='X', my_range_key='Y')

    buf = io.BytesIO()
    pickle.Pickler(buf, protocol=None).dump(obj)
    # print(buf.getbuffer())

    new_obj = pickle.loads(buf.getbuffer())

    assert new_obj.my_hash_key == obj.my_hash_key
    assert new_obj.my_range_key == obj.my_range_key


def test_pickle_rebased_model():
    ensure_class_bases_begin_with(namespace=globals(), class_name=str('MyModel'), base_class=SqliteAdapter)

    obj = MyModel(my_hash_key='X', my_range_key='Y')

    buf = io.BytesIO()
    pickle.Pickler(buf, protocol=None).dump(obj)

    new_obj = pickle.loads(buf.getbuffer())

    assert new_obj.my_hash_key == obj.my_hash_key
    assert new_obj.my_range_key == obj.my_range_key


def test_pickle_subclassed_model():

    ensure_class_bases_begin_with(namespace=globals(), class_name=str('MySubclassedModel'), base_class=SqliteAdapter)

    obj = MySubclassedModel(my_hash_key='X', my_range_key='Y')

    buf = io.BytesIO()
    pickle.Pickler(buf, protocol=None).dump(obj)

    new_obj = pickle.loads(buf.getbuffer())

    assert new_obj.my_hash_key == obj.my_hash_key
    assert new_obj.my_range_key == obj.my_range_key

    print(type(obj), obj)
    # assert 0
