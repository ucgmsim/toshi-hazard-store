from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model


class MyModel(Model):
    __metaclass__ = type

    class Meta:
        table_name = "ModelInModule"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)


class MySubclassedModel(MyModel):
    __metaclass__ = type

    class Meta:
        table_name = "SubclassedModelInModel"

    extra = UnicodeAttribute()
