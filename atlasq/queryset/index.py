from typing import List

import requests
from requests.auth import HTTPDigestAuth

ATLAS_BASE_URL = "https://cloud.mongodb.com/api/atlas/v1.0"
LIST_TEXT_INDEXES_ENDPOINT = (
    ATLAS_BASE_URL
    + "/groups/{GROUP_ID}/clusters/{CLUSTER_NAME}/fts/indexes/{DATABASE_NAME}/{COLLECTION_NAME}"
)


class AtlasIndex:
    def __init__(self, index_name: str):
        self._indexed_fields: List[str] = []
        self.ensured: bool = False
        self._index: str = index_name

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
            raise ValueError("No index defined")
        url = LIST_TEXT_INDEXES_ENDPOINT.format(
            GROUP_ID=group_id,
            CLUSTER_NAME=cluster_name,
            DATABASE_NAME=db_name,
            COLLECTION_NAME=collection_name,
        )
        r = requests.get(url, auth=HTTPDigestAuth(user, password))
        r.raise_for_status()
        index_results = r.json()
        for index_result in index_results:
            if index_result["name"] == self.index:
                self._indexed_fields = list(index_result["mappings"]["fields"].keys())
                self.ensured = True
                return True
        else:
            self.ensured = False
            self._indexed_fields.clear()
            return False

    def ensure_keyword_is_indexed(self, keyword: str):
        if not self.ensured:
            raise ValueError("Index not ensured")
        return keyword in self._indexed_fields
