from unittest.mock import patch

from requests import HTTPError

from atlasq.queryset.index import AtlasIndex
from tests.test_base import TestBaseCase


class MockResponse:
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data

    def raise_for_status(self):
        if not 200 <= self.status_code <= 299:
            raise HTTPError(self.status_code)


class TestManager(TestBaseCase):
    def test_ensure_index_exists(self):
        index = AtlasIndex("myindex")
        self.assertFalse(index.ensured)
        with patch(
            "requests.get",
            return_value=MockResponse(
                [
                    {
                        "collectionName": "COLL",
                        "database": "DB",
                        "indexID": "ID",
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "field1": {"type": "boolean"},
                                "field2": {"indexOptions": "docs", "type": "string"},
                            },
                        },
                        "name": "myindex",
                        "status": "STEADY",
                        "synonyms": [],
                    }
                ],
                200,
            ),
        ):
            result = index.ensure_index_exists(
                "user", "password", "project", "cluster", "db", "collection"
            )
            self.assertTrue(result)
            self.assertTrue(index.ensured)
            self.assertCountEqual(index._indexed_fields, ["field1", "field2"])
            self.assertTrue(index.ensure_keyword_is_indexed("field1"))
            self.assertFalse(index.ensure_keyword_is_indexed("field3"))
        with patch(
            "requests.get",
            return_value=MockResponse(
                [
                    {
                        "collectionName": "COLL",
                        "database": "DB",
                        "indexID": "ID",
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "field1": {"type": "boolean"},
                                "field2": {"indexOptions": "docs", "type": "string"},
                            },
                        },
                        "name": "anotherindex",
                        "status": "STEADY",
                        "synonyms": [],
                    }
                ],
                200,
            ),
        ):
            result = index.ensure_index_exists(
                "user", "password", "project", "cluster", "db", "collection"
            )
            self.assertFalse(result)
            self.assertFalse(index.ensured)
            self.assertCountEqual(index._indexed_fields, [])
        with patch(
            "requests.get",
            return_value=MockResponse(
                {
                    "detail": "Current user is not authorized to perform this action.",
                    "error": 401,
                    "errorCode": "USER_UNAUTHORIZED",
                    "parameters": [],
                    "reason": "Unauthorized",
                },
                401,
            ),
        ):
            with self.assertRaises(HTTPError):
                index.ensure_index_exists("user", "password", "group", "cluster")
