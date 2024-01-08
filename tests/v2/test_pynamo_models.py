class TestPynamoMeta(object):
    def test_meta_table_exists(self, adapter_model):
        assert adapter_model.ToshiOpenquakeMeta.exists()

    def test_save_one_meta_object(self, get_one_meta):
        obj = get_one_meta
        obj.save()
        assert obj.vs30 == 350
