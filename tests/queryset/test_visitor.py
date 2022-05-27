import json

from mongoengine import Document, StringField

from atlasq.queryset.node import AtlasQ
from atlasq.queryset.visitor import AtlasQueryCompilerVisitor
from tests.test_base import TestBaseCase


class MyDocument(Document):
    key = StringField()
    key2 = StringField()
    key3 = StringField()
    key4 = StringField()


class TestAtlasQueryCompilerVisitor(TestBaseCase):
    def visit_combination_and_or(self):
        q1 = AtlasQ(key="value")

        q2 = AtlasQ(key2="value2")
        q3 = q1 | q2

        q4 = AtlasQ(key4="value4")

        q5 = q3 & q4
        filters, aggregations = q5.accept(AtlasQueryCompilerVisitor(MyDocument))
        self.assertEqual([], aggregations)
        # print(json.dumps(filters, indent=4),)
        self.assertEqual(
            {
                "compound": {
                    "filter": [
                        {
                            "compound": {
                                "should": [
                                    {
                                        "compound": {
                                            "filter": [
                                                {
                                                    "text": {
                                                        "query": "value",
                                                        "path": "key",
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "compound": {
                                            "filter": [
                                                {
                                                    "text": {
                                                        "query": "value2",
                                                        "path": "key2",
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                ],
                                "minimumShouldMatch": 1,
                            }
                        },
                        {
                            "compound": {
                                "filter": [
                                    {"text": {"query": "value4", "path": "key4"}},
                                ]
                            }
                        },
                    ],
                }
            },
            filters,
            json.dumps(filters, indent=4),
        )

    def test_combination_and(self):
        q1 = AtlasQ(key="value", key2="value2")
        q2 = AtlasQ(key3="value3", key4="value4")
        q5 = q1 & q2
        filters, aggregations = q5.accept(AtlasQueryCompilerVisitor(MyDocument))

        self.assertEqual([], aggregations)
        self.assertEqual(
            {
                "compound": {
                    "filter": [
                        {"text": {"path": "key", "query": "value"}},
                        {"text": {"path": "key2", "query": "value2"}},
                        {"text": {"path": "key3", "query": "value3"}},
                        {"text": {"path": "key4", "query": "value4"}},
                    ],
                }
            },
            filters,
            json.dumps(filters, indent=4),
        )

    def test_atlas_q_or(self):
        q1 = AtlasQ(key="value", key2="value2")
        q2 = AtlasQ(key3="value3", key4="value4")
        q5 = q1 | q2
        filters, aggregations = q5.accept(AtlasQueryCompilerVisitor(MyDocument))
        self.assertEqual([], aggregations)
        self.assertEqual(
            {
                "compound": {
                    "should": [
                        {
                            "compound": {
                                "filter": [
                                    {"text": {"path": "key", "query": "value"}},
                                    {"text": {"path": "key2", "query": "value2"}},
                                ]
                            }
                        },
                        {
                            "compound": {
                                "filter": [
                                    {"text": {"path": "key3", "query": "value3"}},
                                    {"text": {"path": "key4", "query": "value4"}},
                                ]
                            }
                        },
                    ],
                    "minimumShouldMatch": 1,
                }
            },
            filters,
            json.dumps(filters, indent=4),
        )
