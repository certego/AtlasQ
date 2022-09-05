import fnmatch
from logging import getLogger
from typing import Dict, List

import requests
from requests.auth import HTTPDigestAuth

from atlasq.queryset.exceptions import AtlasIndexError

logger = getLogger(__name__)

ATLAS_BASE_URL = "https://cloud.mongodb.com/api/atlas/v1.0"
LIST_TEXT_INDEXES_ENDPOINT = (
    ATLAS_BASE_URL
    + "/groups/{GROUP_ID}/clusters/{CLUSTER_NAME}/fts/indexes/{DATABASE_NAME}/{COLLECTION_NAME}"
)


class AtlasIndex:

    fields_to_copy = ["ensured", "_indexed_fields", "use_embedded_documents"]

    def __init__(self, index_name: str, use_embedded_documents: bool = True):
        self._indexed_fields: List[str] = []
        self.ensured: bool = False
        self.use_embedded_documents: bool = use_embedded_documents
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
        if index_result["type"] == "document":
            if index_result.get("dynamic", False):
                self._indexed_fields.append(f"{base_field}.*" if base_field else "*")
            else:
                for field, value in index_result.get("fields", {}).items():
                    field = f"{base_field}.{field}" if base_field else field
                    self._set_indexed_fields(value, base_field=field)

        else:
            assert base_field
            self._indexed_fields.append(base_field)

    def _set_indexed_from_mappings(self, index_result: Dict):
        mappings = index_result["mappings"]
        mappings["type"] = "document"
        self._set_indexed_fields(mappings)
        logger.debug(self._indexed_fields)

    def ensure_keyword_is_indexed(self, keyword: str):
        if not self.ensured:
            raise AtlasIndexError("Index not ensured")
        return any(fnmatch.fnmatch(keyword, field) for field in self._indexed_fields)
