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
            self._search_result = self.__collection_aggregate(self._aggrs)
            if self.save_execution_time:
                execution_time = datetime.now() - nnow
                logger.info(
                    f"{round(execution_time.total_seconds(), 3)} - {self._aggrs_query}"
                )
        if not self._return_objects:
            self._cursor_obj = self._search_result
        cursor = super()._cursor
        return cursor

    def order_by(self, *keys):
        if not keys:
            return self
        qs: AtlasQuerySet = self.clone()
        order_by: List[
            Tuple[str, int]
        ] = qs._get_order_by(  # pylint: disable=protected-access
            keys
        )
        aggregation = {"$sort": dict(order_by)}
        qs._other_aggregations.append(aggregation)  # pylint: disable=protected-access
        return qs

    def __getitem__(self, key):
        if isinstance(key, int):
            from mongoengine.queryset.base import BaseQuerySet

            qs = self.clone()
            qs._limit = 1
            return BaseQuerySet.__getitem__(qs, key)
        return super().__getitem__(key)

    @property
    def _query(self):
        if not self._search_result:
            return None
        start = self._skip
        end = self._limit
        # unfortunately here we have to actually run the query to get the objects
        # I do not see other way to do this atm
        ids: List[str] = []
        for i, obj in enumerate(self._search_result):
            if end is not None and i >= end:
                break
            if start is not None and i < start:
                continue
            if obj:
                ids.append(obj["_id"])
        self._query_obj = Q(id__in=ids)
        logger.debug(self._query_obj.to_query(self._document))
        return super()._query

    def __collection_aggregate(self, final_pipeline, **kwargs):
        collection = self._collection
        if self._read_preference is not None or self._read_concern is not None:
            collection = self._collection.with_options(
                read_preference=self._read_preference, read_concern=self._read_concern
            )
        logger.debug(final_pipeline)
        return collection.aggregate(final_pipeline, cursor={}, **kwargs)

    def aggregate(self, pipeline, **kwargs):  # pylint: disable=arguments-differ
        self._return_objects = False
        if isinstance(pipeline, dict):
            pipeline = [pipeline]

        final_pipeline = self._aggrs + pipeline
        return self.__collection_aggregate(final_pipeline)

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
        self._count = True  # pylint: disable=protected-access
        cursor = self.__collection_aggregate(
            self._aggrs
        )  # pylint: disable=protected-access
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

    def limit(self, n):
        qs = self.clone()
        qs._limit = n  # pylint: disable=protected-access
        qs._other_aggregations.append({"$limit": n})  # pylint: disable=protected-access
        return qs
