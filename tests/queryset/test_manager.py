from mongoengine import Document, QuerySet, fields

from atlasq.queryset.manager import AtlasManager
from atlasq.queryset.queryset import AtlasQuerySet
from tests.test_base import TestBaseCase


class TestManager(TestBaseCase):
    def test_with_index(self):
        class MyDocument(Document):
            name = fields.StringField(required=True)
            atlas = AtlasManager("myindex")

        self.assertIsInstance(MyDocument.atlas, AtlasQuerySet)
        self.assertEqual(MyDocument.atlas.index.index, "myindex")
        # self.assertIsNotNone(MyDocument.atlas.cache)

    def test_no_index(self):
        class MyDocument(Document):
            name = fields.StringField(required=True)
            atlas = AtlasManager(None)

        self.assertIsInstance(MyDocument.atlas, QuerySet)
        with self.assertRaises(AssertionError):
            self.assertIsInstance(MyDocument.atlas, AtlasQuerySet)
