import fnmatch
from enum import Enum
from logging import getLogger
from typing import Dict, List

import requests
from requests.auth import HTTPDigestAuth

from atlasq.queryset.exceptions import AtlasIndexError, AtlasIndexFieldError

logger = getLogger(__name__)

ATLAS_BASE_URL = "https://cloud.mongodb.com/api/atlas/v1.0"
TEXT_INDEXES_ENDPOINT = (
    ATLAS_BASE_URL + "/groups/{GROUP_ID}/clusters/{CLUSTER_NAME}/fts/indexes"
)

LIST_TEXT_INDEXES_ENDPOINT = (
    TEXT_INDEXES_ENDPOINT + "/{DATABASE_NAME}/{COLLECTION_NAME}"
)


class AtlasIndexType(Enum):
    DOCUMENT = "document"
    EMBEDDED_DOCUMENT = "embeddedDocuments"
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    OBJECT_ID = "objectId"

    @classmethod
    def values(cls) -> List[str]:
        return [e.value for e in cls]


class AtlasIndex:

    fields_to_copy = ["ensured", "_indexed_fields"]

    def __init__(self, index_name: str):
        self._indexed_fields: Dict[str, str] = {}
        self.ensured: bool = False
        self._index: str = index_name

    def __copy__(self):
        res = AtlasIndex(self._index)
        for field in self.fields_to_copy:
            setattr(res, field, getattr(self, field))
        return res

    @property
    def index(self) -> str:
        return self._index

    @index.setter
    def index(self, index: str):
        self._index = index
        self.ensured = False

    def upload_index(
        self,
        data: Dict,
        user: str,
        password: str,
        group_id: str,
        cluster_name: str,
    ):
        if not self.index:
            raise AtlasIndexError("No index defined")

        if not isinstance(data, dict):
            raise AtlasIndexError("The index should be a dictionary")
        if "mappings" not in data:
            raise AtlasIndexError("There is no 'mappings' in the index")
        if not isinstance(data["mappings"], dict):
            raise AtlasIndexError("The mappings keyword should be a dictionary")
        if "fields" not in data["mappings"]:
            raise AtlasIndexError("There is no 'mappings' in the index")
        if not isinstance(data["mappings"]["fields"], dict):
            raise AtlasIndexError("The fields keyword should be a dictionary")

        if any(id_keyword in data["mappings"]["fields"] for id_keyword in ["id", "pk"]):
            if "_id" not in data["mappings"]["fields"]:
                data["mappings"]["fields"]["_id"] = {
                    "type": AtlasIndexType.OBJECT_ID.value
                }

        url = TEXT_INDEXES_ENDPOINT.format(
            GROUP_ID=group_id,
            CLUSTER_NAME=cluster_name,
        )
        response = requests.post(
            url,
            json=data,
            auth=HTTPDigestAuth(user, password),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()

    def ensure_index_exists(
        self,
        user: str,
        password: str,
        group_id: str,
        cluster_name: str,
        db_name: str,
        collection_name: str,
    ):
        if not self.index:
            raise AtlasIndexError("No index defined")
        url = LIST_TEXT_INDEXES_ENDPOINT.format(
            GROUP_ID=group_id,
            CLUSTER_NAME=cluster_name,
            DATABASE_NAME=db_name,
            COLLECTION_NAME=collection_name,
        )
        response = requests.get(url, auth=HTTPDigestAuth(user, password))
        response.raise_for_status()
        index_results = response.json()
        self._indexed_fields.clear()
        for index_result in index_results:
            if index_result["name"] == self.index:
                self._set_indexed_from_mappings(index_result)
                self.ensured = True
                break
        else:
            self.ensured = False
        return self.ensured

    def _set_indexed_fields(self, index_result: Dict, base_field: str = ""):
        lucene_type = index_result["type"]
        if lucene_type in [
            AtlasIndexType.DOCUMENT.value,
            AtlasIndexType.EMBEDDED_DOCUMENT.value,
        ]:
            if not index_result.get("dynamic", False):
                for field, value in index_result.get("fields", {}).items():
                    field = f"{base_field}.{field}" if base_field else field
                    self._set_indexed_fields(value, base_field=field)
            else:
                self._indexed_fields[f"{base_field}.*" if base_field else "*"] = ""
        if base_field:
            if lucene_type not in AtlasIndexType.values():
                logger.warning(f"Lucene type {lucene_type} not configured")
            self._indexed_fields[base_field] = lucene_type

    def _set_indexed_from_mappings(self, index_result: Dict):
        mappings = index_result["mappings"]
        mappings["type"] = AtlasIndexType.DOCUMENT.value
        self._set_indexed_fields(mappings)
        logger.debug(self._indexed_fields)

    def ensure_keyword_is_indexed(self, keyword: str):
        if not self.ensured:
            raise AtlasIndexError("Index not ensured")
        return any(fnmatch.fnmatch(keyword, field) for field in self._indexed_fields)

    def get_type_from_keyword(self, keyword) -> str:
        if not self.ensured:
            raise AtlasIndexError("Index not ensured")
        if keyword in self._indexed_fields:
            return self._indexed_fields[keyword]
        raise AtlasIndexFieldError(f"Keyword {keyword} not present in index")
