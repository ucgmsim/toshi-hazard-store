import io
import pickle
import pytest

from toshi_hazard_store import model
from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter


def test_pickle_pyanmodb_rlz_model(get_one_rlz):

    obj = get_one_rlz()
    print("type(model.OpenquakeRealization) : ", type(model.OpenquakeRealization))

    # assert 0
    buf = io.BytesIO()
    pickle.Pickler(buf, protocol=None).dump(obj)
    new_obj = pickle.loads(buf.getbuffer())

    assert new_obj.sort_key == obj.sort_key
    assert new_obj.partition_key == obj.partition_key
    assert new_obj.vs30 == obj.vs30
    assert new_obj.values[0].vals[0] == obj.values[0].vals[0]


def test_pickle_rebased_rlz_model(get_one_rlz):
    ensure_class_bases_begin_with(
            namespace=model.__dict__,
            class_name=str('OpenquakeRealization'),  # `str` type differs on Python 2 vs. 3.
            base_class=SqliteAdapter,
    )
    obj = get_one_rlz()


    print("type(model.OpenquakeRealization) : ", type(model.OpenquakeRealization))

    buf = io.BytesIO()
    pickle.Pickler(buf, protocol=None).dump(obj)

    new_obj = pickle.loads(buf.getbuffer())

    assert new_obj.sort_key == obj.sort_key
    assert new_obj.partition_key == obj.partition_key
    assert new_obj.vs30 == obj.vs30
    assert new_obj.values[0].vals[0] == obj.values[0].vals[0]

