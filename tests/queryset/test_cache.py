import datetime
from functools import cached_property
from unittest.mock import MagicMock

from mongoengine import Document, fields
from mongoengine.context_managers import switch_collection, switch_db

from atlasq.queryset.cache import AtlasCache, AtlasDbCache, AtlasRamCache
from tests.test_base import TestBaseCase


class MyDocument(Document):
    name = fields.StringField(required=True)
    md5 = fields.StringField(required=True)
    classification = fields.StringField(required=True)


class TestAtlasCache(TestBaseCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.aggregations = [{"mamma": "mia"}]

    def tearDown(self) -> None:
        self.cache.remove(self.aggregations)

    @cached_property
    def cache(self):
        raise NotImplementedError("Subclasses must implement this method")

    def setUp(self) -> None:
        MyDocument.objects.all().delete()
        self.db_alias = "default"
        self.key = self.cache._aggregations_to_key(self.aggregations)
        self.doc = MyDocument(
            name="test.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
        )


class TestAtlasDbCache(TestAtlasCache):
    @cached_property
    def cache(self):
        return AtlasDbCache(MyDocument, MyDocument._get_collection(), self.db_alias)

    def setUp(self):
        super().setUp()
        self.collection = self.cache.get_collection_name(self.aggregations)

    def test_remove(self):
        self.doc.switch_db(self.db_alias)
        self.doc.switch_collection(self.collection)
        self.doc.save()
        with switch_db(MyDocument, self.db_alias) as Obs:
            with switch_collection(Obs, self.collection) as Collection:
                self.assertEqual(1, Collection.objects.count())
        self.cache.remove(self.aggregations)
        with switch_db(MyDocument, self.db_alias) as Obs:
            with switch_collection(Obs, self.collection) as Collection:
                self.assertEqual(0, Collection.objects.count())

    def test_set_expire_to_collection(self):
        self.cache.set_collection_expiration(self.aggregations, max_minutes=2)
        with switch_collection(MyDocument, self.collection) as Obs:
            self.assertIsNotNone(Obs._meta.get("expire_at", None))

    def test_get(self):
        with self.assertRaises(self.cache.KeyError):
            self.cache.get(self.aggregations)
        self.doc.switch_collection(self.collection)
        self.doc.save()
        with switch_collection(MyDocument, self.collection) as Obs:
            self.assertEqual(1, Obs.objects.count())
        self.cache.set_collection_expiration(self.aggregations, max_minutes=-3)
        with self.assertRaises(self.cache.ExpiredError):
            self.cache.get(self.aggregations)
        self.cache.set_collection_expiration(self.aggregations, max_minutes=1)
        obs2 = MyDocument(
            name="test2.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
        )
        obs2.switch_collection(self.collection)
        obs2.save()

        obs = self.cache.get(self.aggregations)
        self.assertEqual(obs[0].name, obs2.name)


class TestAtlasRamCache(TestAtlasCache):
    @cached_property
    def cache(self):
        return AtlasRamCache(MyDocument, MyDocument._get_collection())

    def test_set(self):
        self.cache.set(self.aggregations, [self.doc], max_minutes=2)
        self.assertEqual([self.doc], self.cache.get(self.aggregations))

    def test_set_with_saved_object(self):
        obs = self.doc.save()
        self.assertEqual(1, MyDocument.objects.count())
        self.cache.set(self.aggregations, [obs], max_minutes=1)
        self.assertEqual(1, MyDocument.objects.count())

    def test_remove(self):
        self.cache.set(self.aggregations, [self.doc], max_minutes=1)
        self.cache.remove(self.aggregations)
        with self.assertRaises(self.cache.KeyError):
            self.cache.get(self.aggregations)

    def test_get(self):
        with self.assertRaises(self.cache.KeyError):
            self.cache.get(self.aggregations)
        self.cache.set(self.aggregations, [self.doc], max_minutes=-2)
        self.doc = MyDocument(
            name="test.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
        )
        self.cache.set(self.aggregations, [self.doc], max_minutes=-2)
        with self.assertRaises(self.cache.ExpiredError):
            self.cache.get(self.aggregations)
        self.cache.set(self.aggregations, [self.doc], max_minutes=2)
        obs = self.cache.get(self.aggregations)
        self.assertEqual(obs, [self.doc])


class TestAtlasCache(TestAtlasCache):
    @cached_property
    def cache(self):
        return AtlasCache(MyDocument, MyDocument._get_collection(), self.db_alias)

    def setUp(self):
        super().setUp()
        self.collection = self.cache.get_collection_name(self.aggregations)

    def test_remove(self):
        self.doc.switch_db(self.db_alias)
        self.doc.switch_collection(self.collection)
        self.doc.save()
        with switch_collection(MyDocument, self.collection) as Collection:
            self.assertEqual(1, Collection.objects.count())
        self.cache._cache[self.key] = (datetime.datetime.now(), [self.doc])
        self.cache.remove(self.aggregations)

        self.assertEqual({}, self.cache._cache)
        with switch_collection(MyDocument, self.collection) as Collection:
            self.assertEqual(0, Collection.objects.count())

    def test_get_ram(self):
        self.cache._cache[self.key] = (
            datetime.datetime.now() + datetime.timedelta(minutes=1),
            [self.doc],
        )
        self.assertEqual([self.doc], self.cache.get(self.aggregations))

    def test_get_db(self):
        with switch_collection(MyDocument, self.collection) as Collection:
            self.assertEqual(0, Collection.objects.count())
        self.doc.switch_collection(self.collection)
        self.doc.save()
        self.cache.set_collection_expiration(self.aggregations, 1)
        real_func = AtlasDbCache.get
        AtlasDbCache.get = MagicMock(side_effect=AtlasDbCache.get)
        self.assertEqual(self.doc.name, self.cache.get(self.aggregations)[0].name)
        AtlasDbCache.get.assert_called()
        # now it should be set the ram cache
        AtlasDbCache.get = MagicMock(side_effect=AtlasDbCache.get)
        self.assertEqual(self.doc.name, self.cache.get(self.aggregations)[0].name)
        AtlasDbCache.get.assert_not_called()
        AtlasDbCache.get = real_func
