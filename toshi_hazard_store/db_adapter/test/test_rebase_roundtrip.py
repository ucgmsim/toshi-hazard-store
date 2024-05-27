from toshi_hazard_store.db_adapter.dynamic_base_class import ensure_class_bases_begin_with, set_base_class


class A:
    my_hash_key = "A"

    def intro(self):
        return type(self).__name__


class B:
    my_hash_key = "B"

    def intro(self):
        return type(self).__name__


class C(A):
    pass
    __metaclass__ = type


def test_simple_class_props():
    a = A()
    b = B()
    assert a.intro() == 'A'
    assert b.intro() == 'B'
    assert a.my_hash_key == 'A'


def test_subclass_props():
    c = C()
    assert c.intro() == 'C'
    assert c.my_hash_key == 'A'
    assert isinstance(c, A) & isinstance(c, C)
    assert not isinstance(c, B)


def test_subclass_ensure_new_base():
    ensure_class_bases_begin_with(namespace=globals(), class_name=str('C'), base_class=B)
    c = C()
    assert isinstance(c, B) & isinstance(c, C) & isinstance(c, A)
    assert c.intro() == 'C'
    assert c.my_hash_key == 'B'


def test_subclass_set_base_class():
    # assert C().my_hash_key == 'A' we can't know what it waws before now, these tests are screwing it up
    set_base_class(namespace=globals(), class_name=str('C'), base_class=B)
    c = C()
    assert isinstance(c, B) & isinstance(c, C) & (not isinstance(c, A))
    assert c.intro() == 'C'
    assert c.my_hash_key == 'B'
