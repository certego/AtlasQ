import copy
import logging
from typing import Any, Dict, List, Tuple

from mongoengine import Q, QuerySet
from pymongo.command_cursor import CommandCursor

from atlasq.queryset import AtlasIndexError
from atlasq.queryset.index import AtlasIndex
from atlasq.queryset.node import AtlasQ

logger = logging.getLogger(__name__)


class AtlasQuerySet(QuerySet):
    def _clone_into(self, new_qs):
        copy_props = (
            "index",
            "_aggrs_query",
            "_search_result",
            "_count",
            "_return_objects",
            "save_execution_time",
            "_other_aggregations",
        )
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
        self._return_objects: bool = True
        self.save_execution_time: bool = False
        self._other_aggregations: List[Dict] = []

    def ensure_index(self, user: str, password: str, group_id: str, cluster_name: str):
        db_name = self._document._get_db().name  # pylint: disable=protected-access
        collection_name = (
            self._document._get_collection_name()  # pylint: disable=protected-access
        )
        return self.index.ensure_index_exists(
            user, password, group_id, cluster_name, db_name, collection_name
        )

    def __iter__(self):
        if not self._return_objects:
            return iter(self._cursor)
        return super().__iter__()

    @property
    def _aggrs(self):
        # corresponding of _query for us
        if self._aggrs_query is None:
            self._aggrs_query = self._query_obj.to_query(self._document)
            if self._aggrs_query:
                if self._count:
                    self._aggrs_query[0]["$search"]["count"] = {"type": "total"}
            self._aggrs_query += self._get_projections()
            self._aggrs_query += self._other_aggregations
            if not self.save_execution_time:
                logger.info(self._aggrs_query)
        return self._aggrs_query

    @property
    def _cursor(self):
        if not self._search_result:
            if self.save_execution_time:
                from datetime import datetime

                nnow = datetime.now()
            self._search_result = super().aggregate(self._aggrs)
            if self.save_execution_time:
                execution_time = datetime.now() - nnow
                logger.info(
                    f"Execution time is {execution_time.total_seconds()} seconds for query {self._aggrs_query}"
                )
        if not self._return_objects:
            self._cursor_obj = self._search_result
        cursor = super()._cursor
        if self.save_execution_time:
            cursor.execution_time = execution_time
        return cursor

    def order_by(self, *keys):
        other = self.clone()
        order_by: List[Tuple[str, int]] = other._get_order_by(keys)
        aggregation = {"$sort": {key: value for key, value in order_by}}
        other._other_aggregations.append(aggregation)
        return other

    @property
    def _query(self):
        if not self._search_result:
            return None
        # unfortunately here we have to actually run the query to get the objects
        # i do not see other way to do this atm
        self._query_obj = Q(id__in=[obj["_id"] for obj in self._search_result if obj])
        logger.debug(self._query_obj.to_query(self._document))
        return super()._query

    def aggregate(self, pipeline, *suppl_pipeline, **kwargs):
        self._return_objects = False
        if isinstance(pipeline, dict):
            pipeline = [pipeline]
        return super().aggregate(self._aggrs + pipeline, *suppl_pipeline)

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
        self._count = True  # pylint: disable=protected-access
        cursor = super().aggregate(self._aggrs)  # pylint: disable=protected-access
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
