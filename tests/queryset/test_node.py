from mongoengine import Document, fields

from atlasq.queryset.node import AtlasQ, AtlasQCombination
from tests.test_base import TestBaseCase


class TestAtlasQ(TestBaseCase):
    def test__combine_empty(self):
        q = AtlasQ(field=3)
        q2 = AtlasQ()
        self.assertEqual(q._combine(q2, q.AND), q)

    def test_combine(self):
        q = AtlasQ(field=3)
        q2 = AtlasQ(field2=4)
        self.assertEqual(q._combine(q2, q.AND), AtlasQCombination(q.AND, [q, q2]))
        self.assertEqual(q._combine(q2, q.OR), AtlasQCombination(q.OR, [q, q2]))

    def test_bool(self):
        self.assertFalse(bool(AtlasQ()))
        self.assertTrue(bool(AtlasQ(f=1)))

    def test_to_query(self):
        class MyDocument(Document):
            name = fields.StringField()

        q = AtlasQ(name="test")
        res = q.to_query(MyDocument)
        self.assertIsInstance(res, tuple)
        self.assertEqual(2, len(res))
        filters, aggregations = res
        self.assertIsInstance(filters, dict)
        self.assertIn("compound", filters)
        self.assertIsInstance(aggregations, list)
        self.assertEqual(0, len(aggregations))


class TestAtlasQCombination(TestBaseCase):
    def test_bool(self):
        self.assertFalse(bool(AtlasQCombination(AtlasQCombination.AND, [AtlasQ()])))
        self.assertTrue(bool(AtlasQCombination(AtlasQCombination.AND, [AtlasQ(f=1)])))

    def test__combine(self):
        q1 = AtlasQCombination(
            AtlasQCombination.AND, [AtlasQ(field=3), AtlasQ(field2=4)]
        )
        q2 = AtlasQCombination(
            AtlasQCombination.AND, [AtlasQ(field3=3), AtlasQ(field4=4)]
        )
        self.assertIsInstance(q1._combine(q2, q1.AND), AtlasQCombination)
