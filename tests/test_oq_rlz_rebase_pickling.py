import io
import pickle

from toshi_hazard_store.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter
from toshi_hazard_store.model import location_indexed_model, openquake_models


def test_pickle_pyanmodb_rlz_model(get_one_rlz):

    obj = get_one_rlz()
    print("type(openquake_models.OpenquakeRealization) : ", type(openquake_models.OpenquakeRealization))

    # assert 0
    buf = io.BytesIO()
    pickle.Pickler(buf, protocol=None).dump(obj)
    new_obj = pickle.loads(buf.getbuffer())

    assert new_obj.sort_key == obj.sort_key
    assert new_obj.partition_key == obj.partition_key
    assert new_obj.vs30 == obj.vs30
    assert new_obj.values[0].vals[0] == obj.values[0].vals[0]


def test_pickle_adapted_rlz_model(adapted_rlz_model, get_one_rlz):
    """the conftest fixture is picklable"""

    obj = get_one_rlz()

    print("type(openquake_models.OpenquakeRealization) : ", type(openquake_models.OpenquakeRealization))

    buf = io.BytesIO()
    pickle.Pickler(buf, protocol=None).dump(obj)

    new_obj = pickle.loads(buf.getbuffer())

    assert new_obj.sort_key == obj.sort_key
    assert new_obj.partition_key == obj.partition_key
    assert new_obj.vs30 == obj.vs30
    assert new_obj.values[0].vals[0] == obj.values[0].vals[0]


def test_pickle_rebased_rlz_model(get_one_rlz):
    """the rebased model is picklable"""
    ensure_class_bases_begin_with(
        namespace=location_indexed_model.__dict__, class_name=str('LocationIndexedModel'), base_class=SqliteAdapter
    )
    ensure_class_bases_begin_with(
        namespace=openquake_models.__dict__,
        class_name=str('OpenquakeRealization'),  # `str` type differs on Python 2 vs. 3.
        base_class=location_indexed_model.__dict__['LocationIndexedModel'],
    )

    obj = get_one_rlz(openquake_models.OpenquakeRealization)

    print("type(openquake_models.OpenquakeRealization) : ", type(openquake_models.OpenquakeRealization))

    buf = io.BytesIO()
    pickle.Pickler(buf, protocol=None).dump(obj)

    new_obj = pickle.loads(buf.getbuffer())

    assert new_obj.sort_key == obj.sort_key
    assert new_obj.partition_key == obj.partition_key
    assert new_obj.vs30 == obj.vs30
    assert new_obj.values[0].vals[0] == obj.values[0].vals[0]
