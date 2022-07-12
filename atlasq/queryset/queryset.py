import copy
import logging
from typing import Any, Dict, List

from mongoengine import Q, QuerySet
from pymongo.command_cursor import CommandCursor

from atlasq.queryset import AtlasIndexError
from atlasq.queryset.index import AtlasIndex
from atlasq.queryset.node import AtlasQ

logger = logging.getLogger(__name__)


class AtlasQuerySet(QuerySet):
    def _clone_into(self, new_qs):
        copy_props = ("index", "_aggrs_query", "_search_result", "_count")
        qs = super()._clone_into(new_qs)
        for prop in copy_props:
            val = getattr(self, prop)
            setattr(new_qs, prop, copy.copy(val))
        return qs

    def __init__(self, document, collection):
        super().__init__(document, collection)
        self._query_obj = AtlasQ()
        self.index: AtlasIndex = None

        self._aggrs_query: List[Dict[str, Any]] = None
        self._search_result: CommandCursor = None
        self._count: bool = False

    def ensure_index(self, user: str, password: str, group_id: str, cluster_name: str):
        db_name = self._document._get_db().name  # pylint: disable=protected-access
        collection_name = (
            self._document._get_collection_name()  # pylint: disable=protected-access
        )
        return self.index.ensure_index_exists(
            user, password, group_id, cluster_name, db_name, collection_name
        )

    @property
    def _aggrs(self):
        # corresponding of _query for us
        if self._aggrs_query is None:
            self._aggrs_query = self._query_obj.to_query(self._document)
            if self._aggrs_query:
                if self._count:
                    self._aggrs_query[0]["$search"]["count"] = {"type": "total"}
            self._aggrs_query += self._get_projections()
            logger.debug(self._aggrs_query)
        return self._aggrs_query

    @property
    def _cursor(self):
        if not self._search_result:
            self._search_result = self.aggregate(self._aggrs)
        return super()._cursor

    @property
    def _query(self):
        if not self._search_result:
            return None
        # unfortunately here we have to actually run the query to get the objects
        # i do not see other way to do this atm
        self._query_obj = Q(id__in=[obj["_id"] for obj in self._search_result if obj])
        logger.debug(self._query_obj.to_query(self._document))
        return super()._query

    def __call__(self, q_obj=None, **query):
        if self.index is None:
            raise AtlasIndexError("Index is not set")
        q = AtlasQ(**query)
        if q_obj is not None:
            q &= q_obj
        logger.debug(q)
        qs = super().__call__(q)
        return qs

    def _get_projections(self) -> List[Dict[str, Any]]:
        if self._count:
            if self._query_obj:
                return [{"$project": {"meta": "$$SEARCH_META"}}]
            return [{"$count": "count"}]

        loaded_fields = self._loaded_fields.as_dict()
        logger.debug(loaded_fields)
        if loaded_fields:
            return [{"$project": loaded_fields}]
        return []

    def count(self, with_limit_and_skip=False):
        # todo manage limit and skip
        qs = self.clone()
        qs._count = True  # pylint: disable=protected-access
        cursor = qs.aggregate(qs._aggrs)  # pylint: disable=protected-access
        try:
            count = next(cursor)
        except StopIteration:
            self._len = 0
        else:
            logger.debug(count)
            if self._query_obj:
                self._len = count["meta"]["count"]["total"]
            else:
                self._len = count["count"]
        logger.debug(self._len)
        return self._len
