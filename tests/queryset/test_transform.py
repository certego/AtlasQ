import datetime
import json
from unittest import expectedFailure

from bson import ObjectId
from mongoengine import Document, fields

from atlasq.queryset.exceptions import AtlasFieldError, AtlasIndexFieldError
from atlasq.queryset.index import AtlasIndex
from atlasq.queryset.node import AtlasQ
from atlasq.queryset.transform import AtlasTransform
from tests.test_base import TestBaseCase


class TestTransformSteps(TestBaseCase):
    def test_merge_embedded_documents_to_not_embedded(self):
        list_of_objs = [
            {
                "text": {
                    "query": "aaa",
                    "path": "field.field2.field3",
                }
            }
        ]
        obj = {
            "embeddedDocument": {
                "path": "field",
                "operator": {
                    "compound": {
                        "must": [{"text": {"query": "bbb", "path": "field.field4"}}]
                    }
                },
            }
        }
        AtlasTransform.merge_embedded_documents(obj, list_of_objs)

    def test_merge_embedded_documents_multi_level_different_level(self):
        obj = {
            "embeddedDocument": {
                "path": "field",
                "operator": {
                    "compound": {
                        "must": [
                            {
                                "embeddedDocument": {
                                    "path": "field.field2",
                                    "operator": {
                                        "compound": {
                                            "must": [
                                                {
                                                    "text": {
                                                        "query": "aaa",
                                                        "path": "field.field2.field3",
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                }
                            }
                        ]
                    }
                },
            }
        }

        list_of_objs = [
            {
                "embeddedDocument": {
                    "path": "field",
                    "operator": {
                        "compound": {
                            "must": [{"text": {"query": "bbb", "path": "field.field4"}}]
                        }
                    },
                }
            }
        ]
        result = AtlasTransform.merge_embedded_documents(obj, list_of_objs)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result))
        self.assertEqual(
            result[0],
            {
                "embeddedDocument": {
                    "path": "field",
                    "operator": {
                        "compound": {
                            "must": [
                                {"text": {"query": "bbb", "path": "field.field4"}},
                                {
                                    "embeddedDocument": {
                                        "path": "field.field2",
                                        "operator": {
                                            "compound": {
                                                "must": [
                                                    {
                                                        "text": {
                                                            "query": "aaa",
                                                            "path": "field.field2.field3",
                                                        }
                                                    }
                                                ]
                                            }
                                        },
                                    }
                                },
                            ]
                        }
                    },
                }
            },
        )

    @expectedFailure
    def test_merge_embedded_documents_multi_level_same_level(self):
        obj = {
            "embeddedDocument": {
                "path": "field",
                "operator": {
                    "compound": {
                        "must": [
                            {
                                "embeddedDocument": {
                                    "path": "field.field2",
                                    "operator": {
                                        "compound": {
                                            "must": [
                                                {
                                                    "text": {
                                                        "query": "aaa",
                                                        "path": "field.field2.field3",
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                }
                            }
                        ]
                    }
                },
            }
        }
        list_of_objs = [
            {
                "embeddedDocument": {
                    "path": "field",
                    "operator": {
                        "compound": {
                            "must": [
                                {
                                    "embeddedDocument": {
                                        "path": "field.field2",
                                        "operator": {
                                            "compound": {
                                                "must": [
                                                    {
                                                        "text": {
                                                            "query": "aaa",
                                                            "path": "field.field2.field4",
                                                        }
                                                    }
                                                ]
                                            }
                                        },
                                    }
                                }
                            ]
                        }
                    },
                }
            }
        ]
        result = AtlasTransform.merge_embedded_documents(obj, list_of_objs)
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result))
        self.assertEqual(
            result[0],
            {
                "embeddedDocument": {
                    "path": "field",
                    "operator": {
                        "compound": {
                            "must": [
                                {
                                    "embeddedDocument": {
                                        "path": "field.field2",
                                        "operator": {
                                            "compound": {
                                                "must": [
                                                    {
                                                        "text": {
                                                            "query": "aaa",
                                                            "path": "field.field2.field3",
                                                        }
                                                    },
                                                    {
                                                        "text": {
                                                            "query": "aaa",
                                                            "path": "field.field2.field4",
                                                        }
                                                    },
                                                ]
                                            }
                                        },
                                    }
                                },
                            ]
                        }
                    },
                }
            },
        )

    def test_merge_embedded_documents(self):
        obj = {
            "embeddedDocument": {
                "path": "field",
                "operator": {
                    "compound": {
                        "must": [{"text": {"query": "aaa", "path": "field.field2"}}]
                    }
                },
            }
        }
        list_of_objs = []

        result = AtlasTransform.merge_embedded_documents(obj, list_of_objs)
        self.assertEqual(0, len(list_of_objs))
        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result))
        self.assertEqual(obj, result[0])

        list_of_objs = [
            {
                "embeddedDocument": {
                    "path": "field",
                    "operator": {
                        "compound": {
                            "must": [{"text": {"query": "bbb", "path": "field.field3"}}]
                        }
                    },
                }
            }
        ]
        result = AtlasTransform.merge_embedded_documents(obj, list_of_objs)
        self.assertEqual(1, len(result))
        self.assertEqual(
            result[0],
            {
                "embeddedDocument": {
                    "path": "field",
                    "operator": {
                        "compound": {
                            "must": [
                                {"text": {"query": "bbb", "path": "field.field3"}},
                                {"text": {"query": "aaa", "path": "field.field2"}},
                            ]
                        }
                    },
                }
            },
        )

        list_of_objs = [
            {
                "embeddedDocument": {
                    "path": "field",
                    "operator": {
                        "compound": {
                            "mustNot": [
                                {"text": {"query": "bbb", "path": "field.field3"}}
                            ]
                        }
                    },
                }
            }
        ]
        result = AtlasTransform.merge_embedded_documents(obj, list_of_objs)
        self.assertEqual(1, len(result))
        self.assertEqual(
            result[0],
            {
                "embeddedDocument": {
                    "path": "field",
                    "operator": {
                        "compound": {
                            "must": [
                                {"text": {"query": "aaa", "path": "field.field2"}}
                            ],
                            "mustNot": [
                                {"text": {"query": "bbb", "path": "field.field3"}}
                            ],
                        }
                    },
                }
            },
        )

    def test__multiple_check_single_embedded_document(self):
        index = AtlasIndex("test")
        index.ensured = True
        index._indexed_fields = {
            "field": "embeddedDocuments",
            "field.field2": "string",
            "field.field3": "string",
        }
        q = AtlasQ(field__field2="aaa", field__field3__ne="bbb")
        try:
            result = AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)
        else:
            print(result[0][0])
            self.assertEqual(
                result[0][0],
                {
                    "embeddedDocument": {
                        "path": "field",
                        "operator": {
                            "compound": {
                                "must": [
                                    {"text": {"query": "aaa", "path": "field.field2"}}
                                ],
                                "mustNot": [
                                    {"text": {"query": "bbb", "path": "field.field3"}}
                                ],
                            }
                        },
                    }
                },
            )

    def test__embedded_document(self):
        index = AtlasIndex("test")
        index.ensured = True
        index._indexed_fields = {"field": "embeddedDocuments", "field.field2": "string"}
        q = AtlasQ(field__field2="aaa")
        try:
            result = AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)
        else:
            self.assertEqual(1, len(result[0]))
            self.assertEqual(
                result[0][0],
                {
                    "embeddedDocument": {
                        "path": "field",
                        "operator": {
                            "compound": {
                                "must": [
                                    {
                                        "text": {
                                            "query": "aaa",
                                            "path": "field.field2",
                                        },
                                    }
                                ]
                            }
                        },
                    }
                },
            )

        index._indexed_fields = {"field": "document", "field.field2": "string"}
        q = AtlasQ(field__field2="aaa")
        try:
            result = AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)
        else:
            self.assertEqual(
                result[0][0], {"text": {"query": "aaa", "path": "field.field2"}}
            )

        index._indexed_fields = {
            "field": "document",
            "field.field2": "document",
            "field.field2.field3": "string",
        }
        q = AtlasQ(field__field2__field3="aaa")
        try:
            result = AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)
        else:
            self.assertEqual(
                result[0][0], {"text": {"query": "aaa", "path": "field.field2.field3"}}
            )

        index._indexed_fields = {
            "field": "document",
            "field.field2": "embeddedDocuments",
            "field.field2.field3": "string",
        }
        q = AtlasQ(field__field2__field3="aaa")
        try:
            result = AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)
        else:
            self.assertEqual(
                result[0][0], {"text": {"query": "aaa", "path": "field.field2.field3"}}
            )

        index._indexed_fields = {
            "field": "embeddedDocuments",
            "field.field2": "embeddedDocuments",
            "field.field2.field3": "string",
        }
        q = AtlasQ(field__field2__field3="aaa")
        try:
            result = AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)
        else:
            self.assertEqual(
                result[0][0],
                {
                    "embeddedDocument": {
                        "path": "field",
                        "operator": {
                            "compound": {
                                "must": [
                                    {
                                        "embeddedDocument": {
                                            "path": "field.field2",
                                            "operator": {
                                                "compound": {
                                                    "must": [
                                                        {
                                                            "text": {
                                                                "query": "aaa",
                                                                "path": "field.field2.field3",
                                                            }
                                                        },
                                                    ]
                                                }
                                            },
                                        }
                                    },
                                ]
                            }
                        },
                    }
                },
            )

        index._indexed_fields = {
            "field": "embeddedDocuments",
            "field.field2": "document",
            "field.field2.field3": "string",
        }
        q = AtlasQ(field__field2__field3="aaa")
        try:
            result = AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)
        else:
            self.assertEqual(
                result[0][0],
                {
                    "embeddedDocument": {
                        "path": "field",
                        "operator": {
                            "compound": {
                                "must": [
                                    {
                                        "text": {
                                            "query": "aaa",
                                            "path": "field.field2.field3",
                                        }
                                    },
                                ]
                            }
                        },
                    }
                },
            )

    def test__ensure_keyword(self):

        index = AtlasIndex("test")
        index._indexed_fields = {"field": "string"}
        q = AtlasQ(field="aaa")
        try:
            AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)
        q = AtlasQ(field2="aaa")
        try:
            AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)

        index.ensured = True

        with self.assertRaises(AtlasIndexFieldError):
            AtlasTransform(q.query, index).transform()

        q = AtlasQ(field__field2="bbb")

        with self.assertRaises(AtlasIndexFieldError):
            AtlasTransform(q.query, index).transform()

        index._indexed_fields["field.field2"] = "string"
        try:
            AtlasTransform(q.query, index).transform()
        except AtlasIndexFieldError as e:
            self.fail(e)

    def test__regex(self):
        q = AtlasQ(f__regex=".*")
        t = AtlasTransform(q.query, AtlasIndex("test"))
        res = t._regex("f", ".*")
        self.assertIn("regex", res)
        self.assertIn("query", res["regex"])
        self.assertIn(".*", res["regex"]["query"])
        self.assertIn("path", res["regex"])
        self.assertIn("f", res["regex"]["path"])

    def test__queryset_value(self):
        class MyDoc(Document):
            field = fields.StringField()

        MyDoc.objects.create(field="aaa")
        objects = MyDoc.objects.all().values_list("field")

        q = AtlasQ(f=objects)
        positive, negative, aggregations = AtlasTransform(
            q.query, AtlasIndex("test")
        ).transform()

        self.assertEqual(positive, [{"text": {"query": ["aaa"], "path": "f"}}])
        self.assertEqual(negative, [])
        self.assertEqual(aggregations, [])

    def test__exists(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        res = t._exists("field")
        self.assertIn("exists", res)
        self.assertIn("path", res["exists"])
        self.assertEqual(res["exists"]["path"], "field")

    def test__range_date_valid(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        time = datetime.datetime.now()
        res = t._range("field", time, ["lte"])
        self.assertEqual(
            res,
            {
                "range": {
                    "path": "field",
                    "lte": time.replace(microsecond=0),
                }
            },
        )

    def test__range_integer_valid(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        res = t._range("field", 3, ["lte"])
        self.assertEqual(res, {"range": {"path": "field", "lte": 3}})

    def test__range_date_invalid(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        time = datetime.datetime.now()
        with self.assertRaises(AtlasFieldError):
            t._range("field", time, ["wat"])

    def test__range_none(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        with self.assertRaises(AtlasFieldError):
            t._range("field", None, ["lte"])

    def test__range_string(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        with self.assertRaises(AtlasFieldError):
            t._range("field", "3", ["lte"])

    def test__equals_string(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        with self.assertRaises(AtlasFieldError):
            t._equals("field", "aaa")

    def test__equals_bool(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        res = t._equals("field", True)
        self.assertIn("equals", res)
        self.assertIn("path", res["equals"])
        self.assertIn("value", res["equals"])
        self.assertEqual(res["equals"]["path"], "field")
        self.assertEqual(res["equals"]["value"], True)

    def test__equals_object_id(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        id = ObjectId("5e45de3dd2bfea029b68cce2")
        res = t._equals("field", id)
        self.assertIn("equals", res)
        self.assertIn("path", res["equals"])
        self.assertIn("value", res["equals"])
        self.assertEqual(res["equals"]["path"], "field")
        self.assertEqual(res["equals"]["value"], id)

    def test_cast_to_object_id(self):
        id = "5e45de3dd2bfea029b68cce2"
        res = AtlasTransform._cast_to_object_id(id)
        self.assertIsInstance(res, ObjectId)

        id = ObjectId("5e45de3dd2bfea029b68cce2")
        res = AtlasTransform._cast_to_object_id(id)
        self.assertIsInstance(res, ObjectId)

        id = 3
        with self.assertRaises(TypeError):
            AtlasTransform._cast_to_object_id(id)

        id = [ObjectId("5e45de3dd2bfea029b68cce2"), 3]
        with self.assertRaises(TypeError):
            AtlasTransform._cast_to_object_id(id)

        id = [ObjectId("5e45de3dd2bfea029b68cce2"), "5e45de3dd2bfea029b68cce2"]
        res = AtlasTransform._cast_to_object_id(id)
        for i in res:
            self.assertIsInstance(i, ObjectId)

    def test__equals_list_bool(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        res = t._equals("field", [True])
        self.assertIn("compound", res)
        self.assertIn("minimumShouldMatch", res["compound"])
        self.assertEqual(1, res["compound"]["minimumShouldMatch"])
        self.assertIn("should", res["compound"])
        self.assertEqual(1, len(res["compound"]["should"]))
        self.assertIn("equals", res["compound"]["should"][0])
        self.assertIn("path", res["compound"]["should"][0]["equals"])
        self.assertIn("value", res["compound"]["should"][0]["equals"])
        self.assertEqual(res["compound"]["should"][0]["equals"]["path"], "field")
        self.assertEqual(res["compound"]["should"][0]["equals"]["value"], True)

    def test(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        res = t._text("field", "aaa")
        self.assertIn("text", res)
        self.assertIn("path", res["text"])
        self.assertIn("query", res["text"])
        self.assertEqual(res["text"]["path"], "field")
        self.assertEqual(res["text"]["query"], "aaa")

    def test_none(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        with self.assertRaises(AtlasFieldError):
            t._text("field", None)

    def test__size_operator_not_supported(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        with self.assertRaises(NotImplementedError):
            t._size("field", 0, "lte")

    def test__size__not_zero(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        with self.assertRaises(NotImplementedError):
            t._size("field", 3, "ne")

    def test__size_equal_zero(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        res = t._size("field", 0, "eq")
        self.assertIn("$match", res)
        self.assertIn("field", res["$match"])
        self.assertIn("$exists", res["$match"]["field"])
        self.assertIn("$eq", res["$match"]["field"])
        self.assertEqual(res["$match"]["field"]["$exists"], True)
        self.assertCountEqual(res["$match"]["field"]["$eq"], [None, [], ""])

    def test__size_ne_zero(self):
        q = AtlasQ(f=3)
        t = AtlasTransform(q.query, AtlasIndex("test"))
        res = t._size("field", 0, "ne")
        self.assertIn("$match", res)
        self.assertIn("field", res["$match"])
        self.assertIn("$exists", res["$match"]["field"])
        self.assertIn("$ne", res["$match"]["field"])
        self.assertEqual(res["$match"]["field"]["$exists"], True)
        self.assertCountEqual(res["$match"]["field"]["$ne"], [None, [], ""])


class TestAtlasQ(TestBaseCase):
    def test_ids_in(self):
        q1 = AtlasQ(id__in=["5e45de3dd2bfea029b68cce2"])
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual(aggregations, [])
        self.assertEqual(negative, [])
        self.assertEqual(
            {
                "compound": {
                    "should": [
                        {
                            "equals": {
                                "path": "_id",
                                "value": ObjectId("5e45de3dd2bfea029b68cce2"),
                            }
                        }
                    ],
                    "minimumShouldMatch": 1,
                }
            },
            positive[0],
            json.dumps(positive, indent=4, default=str),
        )

    def test_size_val(self):
        q1 = AtlasQ(key__size=1)
        with self.assertRaises(NotImplementedError):
            AtlasTransform(q1.query, AtlasIndex("test")).transform()

    def test_size_ne(self):
        q1 = AtlasQ(key__size__ne=0)
        with self.assertRaises(NotImplementedError):
            AtlasTransform(q1.query, AtlasIndex("test")).transform()

    def test_size_gte(self):
        q1 = AtlasQ(key__not__size__gte=0)
        with self.assertRaises(NotImplementedError):
            AtlasTransform(q1.query, AtlasIndex("test")).transform()

    def test_size_negative(self):
        q1 = AtlasQ(key__not__size=0)
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [])
        self.assertEqual(
            {"$match": {"key": {"$exists": True, "$ne": [None, [], ""]}}},
            aggregations[0],
            json.dumps(aggregations, indent=4),
        )

    def test_size_positive(self):
        q1 = AtlasQ(key__size=0)
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [])
        self.assertEqual(
            {"$match": {"key": {"$exists": True, "$eq": [None, [], ""]}}},
            aggregations[0],
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_not_none(self):
        q1 = AtlasQ(key__nin=["", None])
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
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
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [{"exists": {"path": "key"}}])
        self.assertEqual(
            [],
            aggregations,
        )

    def test_atlas_q_not_exists_2(self):
        q1 = AtlasQ(key__not__exists=False)
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual(positive, [{"exists": {"path": "key"}}])
        self.assertEqual(negative, [])
        self.assertEqual(
            [],
            aggregations,
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_not_exists_1(self):
        q1 = AtlasQ(key__exists=False)
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual(positive, [])
        self.assertEqual(negative, [{"exists": {"path": "key"}}])
        self.assertEqual(
            [],
            aggregations,
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_not_exists_0(self):
        q1 = AtlasQ(key__exists=True)
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual(positive, [{"exists": {"path": "key"}}])
        self.assertEqual(negative, [])
        self.assertEqual(
            [],
            aggregations,
            json.dumps(aggregations, indent=4),
        )

    def test_atlas_q_negative_equals(self):
        q1 = AtlasQ(key__ne=True)
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
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
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
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
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
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
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
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

    def test_atlas_q_not_whole_word(self):
        q1 = AtlasQ(key__not__wholeword="value")
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual([], aggregations)
        self.assertEqual(
            [
                {"text": {"path": "key", "query": "value"}},
            ],
            negative,
        )
        self.assertEqual(
            [],
            positive,
            json.dumps(positive, indent=4),
        )

    def test_atlas_q_field_start_with_keyword(self):
        q1 = AtlasQ(key__in=["value"])
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
        self.assertEqual([], aggregations)
        self.assertEqual([], negative)
        self.assertEqual(
            [
                {"text": {"path": "key", "query": ["value"]}},
            ],
            positive,
            json.dumps(positive, indent=4),
        )

    def test_atlas_q_ne_embedded_document(self):
        index = AtlasIndex("test")
        index._indexed_fields = {
            "f": "embeddedDocuments",
            "f.g": "embeddedDocuments",
            "f.g.h": "string",
        }
        index.ensured = True
        q = AtlasQ(f__g__h__ne="test")
        positive, negative, aggregations = AtlasTransform(q.query, index).transform()
        self.assertEqual(
            positive,
            [
                {
                    "embeddedDocument": {
                        "path": "f",
                        "operator": {
                            "compound": {
                                "must": [
                                    {
                                        "embeddedDocument": {
                                            "path": "f.g",
                                            "operator": {
                                                "compound": {
                                                    "mustNot": [
                                                        {
                                                            "text": {
                                                                "query": "test",
                                                                "path": "f.g.h",
                                                            }
                                                        },
                                                    ]
                                                }
                                            },
                                        }
                                    },
                                ]
                            }
                        },
                    }
                }
            ],
            json.dumps(positive, indent=4),
        )
        self.assertEqual(negative, [])
        self.assertEqual(aggregations, [])

    def test_atlas_q_embedded_document(self):
        q = AtlasQ(f__g__h="test")
        index = AtlasIndex("test")
        index._indexed_fields = {
            "f": "embeddedDocuments",
            "f.g": "embeddedDocuments",
            "f.g.h": "string",
        }
        index.ensured = True
        positive, negative, aggregations = AtlasTransform(q.query, index).transform()
        self.assertEqual(
            positive,
            [
                {
                    "embeddedDocument": {
                        "path": "f",
                        "operator": {
                            "compound": {
                                "must": [
                                    {
                                        "embeddedDocument": {
                                            "path": "f.g",
                                            "operator": {
                                                "compound": {
                                                    "must": [
                                                        {
                                                            "text": {
                                                                "query": "test",
                                                                "path": "f.g.h",
                                                            }
                                                        }
                                                    ]
                                                }
                                            },
                                        }
                                    }
                                ]
                            }
                        },
                    }
                }
            ],
            json.dumps(positive, indent=4),
        )
        self.assertEqual(negative, [])
        self.assertEqual(aggregations, [])

    def test_atlas_q_nin(self):
        q1 = AtlasQ(key__nin=["value", "value2"])
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
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
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
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
        positive, negative, aggregations = AtlasTransform(
            q1.query, AtlasIndex("test")
        ).transform()
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
