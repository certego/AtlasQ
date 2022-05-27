from unittest import skip

from mongoengine import Document, ListField, StringField
from mongomock.command_cursor import CommandCursor

from atlasq.queryset.node import AtlasQ
from atlasq.queryset.queryset import AtlasQuerySet
from tests.test_base import TestBaseCase


class MyDocument(Document):
    name = StringField(required=True)
    md5 = StringField(required=True)
    classification = StringField(required=True)
    related_threat = ListField(StringField())


class TestQuerySet(TestBaseCase):
    def setUp(self) -> None:
        super(TestQuerySet, self).setUp()
        MyDocument.objects.all().delete()
        self.base = AtlasQuerySet(
            MyDocument, MyDocument._get_collection(), cache_expiration=0
        )
        self.base.cache = "default"
        self.obs = MyDocument(
            name="test.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
            related_threat=["phishing"],
        )

    @skip("Not implemented in mongoomock")
    def test_sort_by_count_list(self):
        self.obs.save()
        MyDocument.objects.create(
            name="test.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
            related_threat=["phishing", "not_phishing"],
        )
        objects = self.base.sort_by_count("related_threat")
        objects = list(objects)
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0]["_id"], "phishing")
        self.assertEqual(objects[0]["count"], 2)
        self.assertEqual(objects[1]["_id"], "not_phishing")
        self.assertEqual(objects[1]["count"], 1)

    @skip("Not implemented in mongoomock")
    def test_sort_by_count(self):
        self.obs.save()
        MyDocument.objects.create(
            name="test.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
            related_threat=["phishing", "not_phishing"],
        )
        objects = self.base.sort_by_count("classification")
        objects = list(objects)

        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0]["_id"], "domain")
        self.assertEqual(objects[0]["count"], 2)

    def test_clone(self):
        self.base.filters = [1, 2, 3]
        self.base.aggregations = [1, 2, 4]
        self.base.cache_expiration = 5
        clone = self.base.clone()
        self.assertNotEqual(self.base, clone)
        for key, value in self.base.__dict__.items():
            if (
                isinstance(value, str)
                or isinstance(value, list)
                or isinstance(value, int)
            ):
                self.assertEqual(value, clone.__dict__[key])

    def test_must_fields_to_show(self):
        self.assertCountEqual(
            self.base.must_fields_to_show,
            ["classification", "name", "md5"],
        )

    def test__clone_into(self):
        self.base.filters = [1, 2, 3]
        self.base.aggregations = [1, 2, 4]
        self.base.cache_expiration = 5
        self.base.alias = "aa"
        clone = self.base._clone_into(self.base)
        for field in self.base.copy_props:
            self.assertEqual(getattr(self.base, field), getattr(clone, field))

    def test_cache_expire_in(self):
        clone = self.base.cache_expire_in(5)
        self.assertNotEqual(clone, self.base)
        self.assertEqual(clone.cache_expiration, 5)
        self.assertEqual(self.base.cache_expiration, 0)

    def test_scalar(self):
        self.obs.save()
        self.assertEqual(
            self.base.scalar("name", "classification"), [("test.com", "domain")]
        )

    def test_only(self):
        self.obs.save()
        objs = self.base.only("name", "classification")[0]
        self.assertEqual(objs.name, "test.com")
        self.assertEqual(objs.classification, "domain")
        # md5 is always present
        self.assertEqual(objs.md5, "d8cfbe774890e3b523ce584ce640a452")
        self.assertListEqual(objs.related_threat, [])

        objs = self.base.all()[0]
        self.assertEqual(objs.name, "test.com")
        self.assertListEqual(objs.related_threat, ["phishing"])

    def test_count(self):
        self.assertEqual(0, self.base.count())
        self.obs.save()
        self.assertEqual(1, self.base.count())

    def test_order_by(self):
        MyDocument.objects.create(
            name="test3.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
            related_threat=["phishing"],
        )
        MyDocument.objects.create(
            name="test2.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
            related_threat=["phishing"],
        )

        self.obs.save()
        self.assertEqual(self.base.order_by("+name")[0]["name"], "test.com")
        self.assertEqual(self.base.order_by("-name")[0]["name"], "test3.com")

    def test_aggregate(self):
        self.obs.save()
        cursor = self.base.aggregate(
            [{"$match": {"classification": "domain"}}],
        )
        self.assertIsInstance(cursor, CommandCursor)
        result = next(cursor)
        self.assertEqual(result["_id"], self.obs.id)

    @skip("To mock")
    def test_filter(self):
        objects = self.base.filter(classification="domain")
        self.assertEqual(len(objects), 0)
        self.obs.save()
        objects = self.base.filter(classification="domain")
        self.assertEqual(len(objects), 1)
        objects = self.base.filter(AtlasQ(classification="domain"))
        self.assertEqual(len(objects), 1)

    @skip("To mock")
    def test_filter_cache(self):
        base = self.base.cache_expire_in(5)
        objects = base.filter(classification="domain")
        self.assertEqual(len(objects), 0)
        self.obs.save()
        objects = self.base.filter(classification="domain")
        self.assertEqual(len(objects), 1)
        objects = base.filter(classification="domain")
        self.assertEqual(len(objects), 0)
