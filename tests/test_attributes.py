import os
from pathlib import Path

import numpy as np
import pytest

from toshi_hazard_store.model import AggregationEnum, ProbabilityEnum, VS30Enum
from toshi_hazard_store.model.attributes import (  # EnumConstrainedFloatAttribute,
    CompressedListAttribute,
    EnumAttribute,
    EnumConstrainedIntegerAttribute,
    EnumConstrainedUnicodeAttribute,
)

INVALID_ARGS_LIST = [AggregationEnum.MEAN, object(), 'MEAN', {}]

folder = Path(Path(os.path.realpath(__file__)).parent, 'fixtures', 'disaggregation')
disaggs = np.load(Path(folder, 'deagg_SLT_v8_gmm_v2_FINAL_-39.000~175.930_750_SA(0.5)_86_eps-dist-mag-trt.npy'))


def test_attribute_compression():
    """Test if compressing the numpy array is worthwhile."""
    print("Size of the array: ", disaggs.size)
    print("Memory size of one array element in bytes: ", disaggs.itemsize)
    array_size = disaggs.size * disaggs.itemsize
    print("Memory size of numpy array in bytes:", array_size)

    import pickle
    import sys
    import zlib

    comp = zlib.compress(pickle.dumps(disaggs))
    uncomp = pickle.loads(zlib.decompress(comp))

    assert uncomp.all() == disaggs.all()
    assert sys.getsizeof(comp) < array_size / 5


@pytest.mark.parametrize('invalid_arg', [AggregationEnum, object(), 1.00])
def test_compressed_list_serialise_invalid_type_raises(invalid_arg):
    attr = CompressedListAttribute()

    with pytest.raises(TypeError):
        print(invalid_arg)
        attr.serialize(invalid_arg)


@pytest.mark.parametrize('valid_arg', [[], [1, 2, 3], None])
def test_compressed_list_serialise_valid(valid_arg):
    attr = CompressedListAttribute()
    attr.serialize(valid_arg)


class TestEnumAttribute(object):
    @pytest.mark.skip("is this valid now")
    def test_serialize_an_enum(self):
        attr = EnumAttribute(ProbabilityEnum)
        print(dir(attr))
        print(attr.attr_path)

        print(attr.set(ProbabilityEnum._10_PCT_IN_50YRS))
        # assert attr.serialize(ProbabilityEnum._10_PCT_IN_50YRS) == '_10_PCT_IN_50YRS'
        print(attr)
        assert attr == '_10_PCT_IN_50YRS'
        print(attr.serialize())
        assert 0

    def test_deserialize_an_enum(self):
        attr = EnumAttribute(ProbabilityEnum)
        assert attr.deserialize('_10_PCT_IN_50YRS') == ProbabilityEnum._10_PCT_IN_50YRS

    def test_deserialize_invalid_value_raises_value_err(self):
        attr = EnumAttribute(ProbabilityEnum)
        with pytest.raises(ValueError) as ctx:
            attr.deserialize('EIGHT_PCT_IN_50YRS')
        print(dir(ctx))
        assert "EIGHT_PCT_IN_50YRS" in repr(ctx.value)
        assert "ProbabilityEnum" in repr(ctx.value)

    @pytest.mark.parametrize('invalid_arg', [VS30Enum._450, AggregationEnum, object(), {}])
    def test_set_an_unknown_type_raises_value_err(self, invalid_arg):
        attr = EnumAttribute(AggregationEnum)

        with pytest.raises(ValueError) as ctx:
            attr.set(invalid_arg)
            # attr.serialize(invalid_arg)
        assert 'AggregationEnum' in repr(ctx.value)


class TestEnumConstrainedAttributeIntegerEnums(object):
    def test_serialize_a_valid_integer(self):
        assert VS30Enum(750) == VS30Enum._750
        attr = EnumConstrainedIntegerAttribute(VS30Enum)
        assert attr.serialize(750) == str(VS30Enum._750.value)

    def test_deserialize_a_valid_integer(self):
        attr = EnumConstrainedIntegerAttribute(VS30Enum)
        assert attr.deserialize(750) == VS30Enum._750.value

    @pytest.mark.parametrize('invalid_arg', [123, "123"])
    def test_deserialize_an_unknown_value_raises_value_err(self, invalid_arg):
        attr = EnumConstrainedIntegerAttribute(VS30Enum)
        val = invalid_arg
        with pytest.raises(ValueError) as ctx:
            attr.deserialize(val)
        print(ctx.value)
        print(dir(ctx))
        assert str(val) in repr(ctx.value)

    @pytest.mark.parametrize('invalid_arg', ["123", 123, 'MEAN'])
    def test_serialize_an_unknown_number_raises_value_err(self, invalid_arg):
        attr = EnumConstrainedIntegerAttribute(VS30Enum)
        val = invalid_arg
        with pytest.raises(ValueError) as ctx:
            attr.serialize(val)
        assert str(val) in repr(ctx.value)


class TestEnumConstrainedAttribute(object):
    def test_serialize_a_valid_str(self):
        assert AggregationEnum('mean') == AggregationEnum.MEAN
        attr = EnumConstrainedUnicodeAttribute(AggregationEnum)
        assert attr.serialize('mean') == AggregationEnum.MEAN.value

    def test_deserialize_a_valid_str(self):
        assert AggregationEnum('mean') == AggregationEnum.MEAN
        attr = EnumConstrainedUnicodeAttribute(AggregationEnum)
        assert attr.deserialize('mean') == AggregationEnum.MEAN.value

    def test_serialize_an_unknown_str_raises_value_err(self):
        attr = EnumConstrainedUnicodeAttribute(AggregationEnum)
        val = 'NAHH'
        with pytest.raises(ValueError) as ctx:
            a = attr.serialize(val)
            print(a)

        # print(ctx.exception)
        print(dir(ctx))
        assert val in repr(ctx.value)

    def test_deserialize_an_unknown_str_raises_value_err(self):
        attr = EnumConstrainedUnicodeAttribute(AggregationEnum)
        val = 'NAHH'
        with pytest.raises(ValueError) as ctx:
            attr.deserialize(val)

        # print(ctx.exception)
        print(dir(ctx))
        assert val in repr(ctx.value)

    @pytest.mark.parametrize('invalid_arg', [VS30Enum._450, object(), 'MEAN', {}])
    def test_serialize_an_unknown_type_raises_value_err(self, invalid_arg):
        attr = EnumConstrainedUnicodeAttribute(AggregationEnum)

        with pytest.raises(ValueError) as ctx:
            attr.serialize(invalid_arg)
        assert 'AggregationEnum' in repr(ctx.value)


# @pytest.mark.skip('how?')
@pytest.mark.parametrize(
    'valid_arg',
    [
        "0.005",
        "0.01",
        "0.025",
        "0.05",
        "0.1",
        "0.2",
        "0.3",
        "0.4",
        "0.5",
        "0.6",
        "0.7",
        "0.8",
        "0.9",
        "0.95",
        "0.975",
        "0.99",
        "0.995",
    ],
)
def test_serialise_all_valid_percentiles(valid_arg):
    attr = EnumConstrainedUnicodeAttribute(AggregationEnum)
    assert attr.serialize(valid_arg) == AggregationEnum(valid_arg).value


year_prob_mapping = {
    "_86_PCT_IN_50YRS": 3.8559e-02,
    "_63_PCT_IN_50YRS": 1.9689e-02,
    "_39_PCT_IN_50YRS": 9.8372e-03,
    "_18_PCT_IN_50YRS": 3.9612e-03,
    "_10_PCT_IN_50YRS": 2.1050e-03,
    "_5_PCT_IN_50YRS": 1.0253e-03,
    "_2_PCT_IN_50YRS": 4.0397e-04,
    "_1_PCT_IN_50YRS": 2.0099e-04,
    "_05_PCT_IN_50YRS": 1.0025e-04,
}


# @pytest.mark.skip('how?')
@pytest.mark.parametrize('valid_arg', year_prob_mapping.keys())
def test_serialize_all_valid_probablities(valid_arg):
    attr = EnumAttribute(ProbabilityEnum)
    test_value = ProbabilityEnum[valid_arg]
    print(test_value)
    assert attr.serialize(test_value) == ProbabilityEnum[valid_arg].name


# @pytest.mark.skip('how?')
# @pytest.mark.parametrize('valid_arg', year_prob_mapping.values())
# def test_serialize_all_valid_probablities_by_value(valid_arg):
#     attr = EnumAttribute(ProbabilityEnum)
#     test_value = ProbabilityEnum(valid_arg)
#     print(test_value)
#     assert attr.serialize(test_value) == ProbabilityEnum(valid_arg).name
