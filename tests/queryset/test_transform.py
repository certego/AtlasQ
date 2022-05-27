import datetime
import json

from atlasq.queryset.node import AtlasQ
from atlasq.queryset.transform import AtlasTransform
from tests.test_base import TestBaseCase


class TestTransformSteps(TestBaseCase):
    def test__exists_empty(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        res = t._exists("field", True)
        self.assertIn("$match", res)
        self.assertIn("field", res["$match"])
        self.assertIn("$exists", res["$match"]["field"])
        self.assertIn("$eq", res["$match"]["field"])
        self.assertEqual(res["$match"]["field"]["$exists"], True)
        self.assertCountEqual(res["$match"]["field"]["$eq"], [None, [], ""])

    def test__exists_not_empty(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        res = t._exists("field", False)
        self.assertIn("$match", res)
        self.assertIn("field", res["$match"])
        self.assertIn("$exists", res["$match"]["field"])
        self.assertIn("$ne", res["$match"]["field"])
        self.assertEqual(res["$match"]["field"]["$exists"], True)
        self.assertCountEqual(res["$match"]["field"]["$ne"], [None, [], ""])

    def test__range_date_valid(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        time = datetime.datetime.now()
        res = t._range("field", time, "lte")
        self.assertEqual(
            res,
            {
                "range": {
                    "path": "field",
                    "lte": time.replace(minute=0, second=0, microsecond=0),
                }
            },
        )

    def test__range_integer_valid(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        res = t._range("field", 3, "lte")
        self.assertEqual(res, {"range": {"path": "field", "lte": 3}})

    def test__range_date_invalid(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        time = datetime.datetime.now()
        with self.assertRaises(ValueError):
            t._range("field", time, "wat")

    def test__range_none(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        with self.assertRaises(ValueError):
            t._range("field", None, "lte")

    def test__range_string(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        with self.assertRaises(ValueError):
            t._range("field", "3", "lte")

    def test__equals(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        res = t._equals("field", "aaa")
        self.assertIn("equals", res)
        self.assertIn("path", res["equals"])
        self.assertIn("value", res["equals"])
        self.assertEqual(res["equals"]["path"], "field")
        self.assertEqual(res["equals"]["value"], "aaa")

    def test__text(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        res = t._text("field", "aaa")
        self.assertIn("text", res)
        self.assertIn("path", res["text"])
        self.assertIn("query", res["text"])
        self.assertEqual(res["text"]["path"], "field")
        self.assertEqual(res["text"]["query"], "aaa")

    def test__text_none(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        with self.assertRaises(ValueError):
            t._text("field", None)

    def test__size_operator_not_supported(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        with self.assertRaises(NotImplementedError):
            t._size("field", 0, "lte")

    def test__size__not_zero(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        with self.assertRaises(NotImplementedError):
            t._size("field", 3, "ne")

    def test__size_equal_zero(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        res = t._size("field", 0, "eq")
        self.assertIn("$match", res)
        self.assertIn("field", res["$match"])
        self.assertIn("$exists", res["$match"]["field"])
        self.assertIn("$eq", res["$match"]["field"])
        self.assertEqual(res["$match"]["field"]["$exists"], True)
        self.assertCountEqual(res["$match"]["field"]["$eq"], [None, [], ""])

    def test__size_ne_zero(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query)
        res = t._size("field", 0, "ne")
        self.assertIn("$match", res)
        self.assertIn("field", res["$match"])
        self.assertIn("$exists", res["$match"]["field"])
        self.assertIn("$ne", res["$match"]["field"])
        self.assertEqual(res["$match"]["field"]["$exists"], True)
        self.assertCountEqual(res["$match"]["field"]["$ne"], [None, [], ""])


class TestAtlasQ(TestBaseCase):
    def test_size_val(self):
        q1 = AtlasQ(key__size=1)
        with self.assertRaises(NotImplementedError):
            AtlasTransform(q1.query).transform()

    def test_size_ne(self):
        q1 = AtlasQ(key__size__ne=0)
        with self.assertRaises(NotImplementedError):
            AtlasTransform(q1.query).transform()

    def test_size_gte(self):
        q1 = AtlasQ(key__not__size__gte=0)
        with self.assertRaises(NotImplementedError):
            AtlasTransform(q1.query).transform()

    def test_size_negative(self):
        q1 = AtlasQ(key__not__size=0)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [])
        self.assertEqual(
            {"$match": {"key": {"$exists": True, "$ne": [None, [], ""]}}},
            aggregations[0],
            json.dumps(aggregations, indent=4),
        )

    def test_size_positive(self):
        q1 = AtlasQ(key__size=0)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [])
        self.assertEqual(
            {"$match": {"key": {"$exists": True, "$eq": [None, [], ""]}}},
            aggregations[0],
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_not_none(self):
        q1 = AtlasQ(key__nin=["", None])
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(aggregations, [])
        self.assertEqual(positive, [])
        self.assertEqual(
            [
                {"text": {"path": "key", "query": ["", None]}},
            ],
            negative,
            json.dumps(negative, indent=4),
        )

    def test_atlas_q_not_exists_3(self):
        q1 = AtlasQ(key__not__exists=True)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [])
        self.assertEqual(
            {"$match": {"key": {"$exists": True, "$eq": [None, [], ""]}}},
            aggregations[0],
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_not_exists_2(self):
        q1 = AtlasQ(key__not__exists=False)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [])
        self.assertEqual(
            {"$match": {"key": {"$exists": True, "$ne": [None, [], ""]}}},
            aggregations[0],
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_not_exists_1(self):
        q1 = AtlasQ(key__exists=False)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(
            {"$match": {"key": {"$exists": True, "$eq": [None, [], ""]}}},
            aggregations[0],
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_not_exists_0(self):
        q1 = AtlasQ(key__exists=True)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [])
        self.assertEqual(
            {"$match": {"key": {"$exists": True, "$ne": [None, [], ""]}}},
            aggregations[0],
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_negative_equals(self):
        q1 = AtlasQ(key__ne=True)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(aggregations, [])
        self.assertEqual(positive, [])
        self.assertEqual(
            [
                {"equals": {"path": "key", "value": True}},
            ],
            negative,
            json.dumps(negative, indent=4),
        )

    def test_atlas_q_equals(self):
        q1 = AtlasQ(key=True)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual([], aggregations)
        self.assertEqual([], negative)
        self.assertEqual(
            [
                {"equals": {"path": "key", "value": True}},
            ],
            positive,
            json.dumps(positive, indent=4),
        )

    def test_atlas_q_lt(self):
        date = "2021-01-01T00:00:00.000Z"
        date = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        q1 = AtlasQ(last_sent_time__lt=date)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual([], aggregations)
        self.assertEqual([], negative)
        self.assertEqual(
            [
                {
                    "range": {
                        "path": "last_sent_time",
                        "lt": date,
                    }
                }
            ],
            positive,
            json.dumps(positive, indent=4, default=str),
        )

    def test_atlas_q_gte(self):
        date = "2021-01-01T00:00:00.000Z"
        date = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
        q1 = AtlasQ(last_sent_time__gte=date)
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual([], aggregations)
        self.assertEqual([], negative)
        self.assertEqual(
            [
                {
                    "range": {
                        "path": "last_sent_time",
                        "gte": date,
                    }
                }
            ],
            positive,
            json.dumps(positive, indent=4, default=str),
        )

    def test_atlas_q_field_start_with_keyword(self):
        q1 = AtlasQ(key__internal__in=["value"])
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual([], aggregations)
        self.assertEqual([], negative)
        self.assertEqual(
            [
                {"text": {"path": "key.internal", "query": ["value"]}},
            ],
            positive,
            json.dumps(positive, indent=4),
        )

    def test_atlas_q_ne_embedded_document(self):
        q1 = AtlasQ(key__internal__key__ne="value")
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual([], aggregations)
        self.assertEqual([], positive)
        self.assertEqual(
            [
                {"text": {"path": "key.internal.key", "query": "value"}},
            ],
            negative,
            json.dumps(negative, indent=4),
        )

    def test_atlas_q_embedded_document(self):
        q1 = AtlasQ(key__internal__key="value")
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual([], aggregations)
        self.assertEqual([], negative)
        self.assertEqual(
            [
                {"text": {"path": "key.internal.key", "query": "value"}},
            ],
            positive,
            json.dumps(positive, indent=4),
        )

    def test_atlas_q_nin(self):
        q1 = AtlasQ(key__nin=["value", "value2"])
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(aggregations, [])
        self.assertEqual(positive, [])
        self.assertEqual(
            [
                {"text": {"path": "key", "query": ["value", "value2"]}},
            ],
            negative,
            json.dumps(negative, indent=4),
        )

    def test_atlas_q_negative(self):
        q1 = AtlasQ(key__ne="value")
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual(aggregations, [])
        self.assertEqual(positive, [])

        self.assertEqual(
            [
                {"text": {"path": "key", "query": "value"}},
            ],
            negative,
            json.dumps(negative, indent=4),
        )

    def test_atlas_q(self):
        q1 = AtlasQ(key="value", key2="value2")
        positive, negative, aggregations = AtlasTransform(q1.query).transform()
        self.assertEqual([], aggregations)
        self.assertEqual([], negative)
        self.assertEqual(
            [
                {"text": {"path": "key", "query": "value"}},
                {"text": {"path": "key2", "query": "value2"}},
            ],
            positive,
            json.dumps(positive, indent=4),
        )
