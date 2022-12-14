from unittest.mock import patch

from mongoengine import Document, ListField, StringField
from mongomock import command_cursor
from mongomock.command_cursor import CommandCursor

from atlasq import AtlasManager, AtlasQ
from atlasq.queryset.exceptions import AtlasIndexFieldError
from tests.test_base import TestBaseCase


class MyDocument(Document):
    name = StringField(required=True)
    md5 = StringField(required=True)
    classification = StringField(required=True)
    related_threat = ListField(StringField())

    atlas = AtlasManager("test")


class TestQuerySet(TestBaseCase):
    def setUp(self) -> None:
        super(TestQuerySet, self).setUp()
        MyDocument.objects.all().delete()
        self.base = MyDocument.atlas
        self.obs = MyDocument(
            name="test.com",
            md5="d8cfbe774890e3b523ce584ce640a452",
            classification="domain",
            related_threat=["phishing"],
        )
        self.base.index.ensured = False

    def test_clone(self):
        self.base._count = True
        self.base.index.ensured = True
        qs = self.base.clone()
        self.assertEqual(qs._count, True)
        self.assertEqual(qs._aggrs_query, None)
        self.assertEqual(qs._search_result, None)
        self.assertNotEqual(qs.index, self.base.index)
        self.assertEqual(qs.index.index, self.base.index.index)
        self.assertEqual(qs.index.ensured, self.base.index.ensured)

    def test_ensure_index(self):
        with patch("atlasq.queryset.queryset.AtlasIndex.ensure_index_exists") as mock:
            self.base.ensure_index("user", "password", "group_id", "cluster_name")
            mock.assert_called_once_with(
                "user",
                "password",
                "group_id",
                "cluster_name",
                self.db_name,
                "my_document",
            )

    def test_count(self):
        r = self.base.count()
        self.assertEqual(r, 0)
        self.obs.save()
        r = self.base.count()
        self.assertEqual(r, 1)
        with patch(
            "mongomock.aggregate.process_pipeline",
            side_effect=[
                command_cursor.CommandCursor([{"meta": {"count": {"total": 0}}}]),
            ],
        ):
            r = self.base.filter(name="wrong.com").count()
        self.assertEqual(r, 0)

    def test_order_by(self):
        qs = self.base.order_by("-time")
        self.assertEqual(qs._aggrs[0], {"$sort": {"time": -1}})
        qs = self.base.order_by("+time")
        self.assertEqual(qs._aggrs[0], {"$sort": {"time": 1}})
        qs = self.base.order_by("time")
        self.assertEqual(qs._aggrs[0], {"$sort": {"time": 1}})
        qs = self.base.order_by("-time").filter(name="123")
        self.assertEqual(qs._aggrs[1], {"$sort": {"time": -1}})

    def test_only(self):
        qs = self.base.only("name").filter(name="123").order_by("-time")
        self.assertEqual(qs._get_projections(), [{"$project": {"name": 1}}])
        self.assertEqual(3, len(qs._aggrs))
        self.assertEqual(qs._aggrs[1], {"$project": {"name": 1}})

    def test_exclude(self):
        qs = self.base.exclude("name")
        self.assertEqual(qs._get_projections(), [{"$project": {"name": 0}}])

    def test_filter(self):
        qs = self.base.filter(name="test.com")
        self.assertEqual(
            qs._aggrs,
            [
                {
                    "$search": {
                        "index": "test",
                        "compound": {
                            "filter": [
                                {
                                    "text": {
                                        "query": "test.com",
                                        "path": "name",
                                    }
                                }
                            ]
                        },
                    }
                }
            ],
        )
        with patch(
            "mongomock.aggregate.process_pipeline",
            side_effect=[
                command_cursor.CommandCursor([{}]),
            ],
        ):
            self.assertEqual(len(qs), 0)
        self.obs.save()
        qs = self.base.filter(AtlasQ(name="test.com") & AtlasQ(classification="domain"))
        with patch(
            "mongomock.aggregate.process_pipeline",
            side_effect=[
                command_cursor.CommandCursor(
                    [{"_id": self.obs.id, "name": self.obs.name}]
                ),
            ],
        ):
            self.assertEqual(len(qs), 1)

    def test_aggregate(self):
        cursor = self.base.aggregate({"$match": {"name": "test.com"}})
        self.assertIsInstance(cursor, CommandCursor)
        with self.assertRaises(StopIteration):
            next(cursor)
        self.obs.save()
        cursor = self.base.aggregate({"$match": {"name": "test.com"}})
        self.assertIsInstance(cursor, CommandCursor)
        obj = next(cursor)
        self.assertEqual(obj["_id"], self.obs.id)
        self.assertIn("name", obj)
        # exclude does not work with aggregate, even with mongoengine
        cursor = self.base.exclude("name").aggregate({"$match": {"name": "test.com"}})
        obj = next(cursor)
        self.assertIn("name", obj)

    def test_first(self):
        self.assertIsNone(self.base.first())
        self.obs.save()
        self.assertEqual(self.base.first().id, self.obs.id)
        with patch(
            "mongomock.aggregate.process_pipeline",
            side_effect=[
                command_cursor.CommandCursor(
                    [{"_id": self.obs.id, "name": self.obs.name}]
                ),
            ],
        ):
            self.assertEqual(
                self.base.filter(name="test.com").first().name, self.obs.name
            )

    def test_get(self):
        with patch(
            "mongomock.aggregate.process_pipeline",
            side_effect=[
                command_cursor.CommandCursor([{}]),
            ],
        ):
            with self.assertRaises(MyDocument.DoesNotExist):
                self.base.get(name="test.com")
        self.obs.save()
        with patch(
            "mongomock.aggregate.process_pipeline",
            side_effect=[
                command_cursor.CommandCursor(
                    [{"_id": self.obs.id, "name": self.obs.name}]
                ),
            ],
        ):
            self.assertEqual(self.base.get(name="test.com").name, self.obs.name)
        self.base.index.ensured = True
        with self.assertRaises(AtlasIndexFieldError):
            self.base.get(another_field="test.com")
