import copy
import json
import logging
from typing import Dict, List

from mongoengine import Q, QuerySet
from mongoengine.common import _import_class

from atlasq.queryset.cache import AtlasCache
from atlasq.queryset.node import AtlasQ, AtlasQCombination

logger = logging.getLogger(__name__)


class AtlasQuerySet(QuerySet):
    copy_props = (
        "filters",
        "aggregations",
        "projections",
        "index",
        "fields_to_show",
        "order_by_fields",
        "count_objects",
        "cache_expiration",
        "_cache",
        "alias",
    )
    order_mapping = {
        "+": 1,
        "-": -1,
    }

    def clone(self) -> "AtlasQuerySet":
        return self._clone_into(
            self.__class__(
                self._document,
                self._collection,
                self.cache._db_connection_alias,  # pylint: disable=protected-access
            )
        )

    def _clone_into(self, new_qs) -> "AtlasQuerySet":
        new_qs = super()._clone_into(new_qs)

        for prop in self.copy_props:
            val = getattr(self, prop)
            setattr(new_qs, prop, copy.copy(val))
        return new_qs

    def __repr__(self):
        return repr(self._execute())

    def __init__(self, document, collection, cache_expiration: int = 0):
        super().__init__(document, collection)
        self.filters: List[Dict] = []
        self.aggregations: List[Dict] = []
        self.projections: List[Dict] = []
        # we always have to retrieve at least the required fields of document, it is required by the cache
        self.must_fields_to_show = set(
            k
            for k, v in self._document._fields.items()
            if v.required  # pylint: disable=protected-access
        )
        self.fields_to_show = set()
        self.order_by_fields = set()
        self.count_objects: bool = False

        self._cache = None

        self.cache_expiration = cache_expiration

        self._index = None
        self.alias = "default"

    @property
    def cache(self) -> AtlasCache:
        return self._cache

    @cache.setter
    def cache(self, alias: str):
        self._cache = AtlasCache(self._document, self._collection, alias)

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    def __iter__(self):
        objects = self._execute()
        return iter(objects)

    def cache_expire_in(self, cache_expiration: int):
        qs = self.clone()
        qs.cache_expiration = cache_expiration
        return qs

    def _is_field__list(self, field: str) -> bool:
        ListField = _import_class("ListField")  # pylint: disable=invalid-name
        field_parts = field.split(".")
        field_instances = (
            self._document._lookup_field(  # pylint: disable=protected-access
                field_parts
            )
        )
        return isinstance(field_instances[-1], ListField)

    def unwind(self, field: str):
        logger.debug(f"called unwind for field {field}")
        qs = self.clone()
        qs.aggregations.append({"$unwind": f"${field}"})
        return qs

    def using(self, alias):
        self.alias = alias
        return super().using(alias)

    def sort_by_count(self, field: str):
        logger.debug(f"called sort_by_count for field {field}")
        qs = self.clone()
        if qs._is_field__list(field):  # pylint: disable=protected-access
            qs.aggregations = qs.unwind(field).aggregations
        qs.aggregations.append({"$sortByCount": f"${field}"})
        return qs

    def __out_query(self) -> Dict:
        out = {
            "$out": {
                "db": self.cache.db_name,
                "coll": self.cache.get_collection_name(self.filters),
            }
        }
        return out

    def __get_from_db(self) -> QuerySet:
        # we are redirecting the query result to the cache collection
        # we can call the pymongo aggregation, redirecting the result to the cache collection

        aggregations = self.filters + self.projections
        if self.cache_expiration > 0:
            logger.debug("Redirect output to cache collection")
            aggregations.append(self.__out_query())
            # the return value is an empty cursor
            super().aggregate(aggregations)
            # we need to manually set the cache expiration in the new collection
            self.cache.set_collection_expiration(
                self.filters + self.projections, self.cache_expiration
            )
            # we can then use the cache to retrieve it from the collection that we just created
            # if this does not hit, we have a real problem, since we have populated it one second ago
            objects = self.cache.get(self.filters + self.projections, force=True)
            # we have to retrieve the document from the main db
            query = Q()
            for obj in objects:
                # we are sure that these fields are present,
                # so we can safely use them to retrieve the valid document
                query |= Q(**{field: obj[field] for field in self.must_fields_to_show})
        else:
            logger.debug("No redirection of output to cache collection")
            objects = super().aggregate(aggregations)
            # we have the correct id, so we can just retrieve the document from the main db
            query = Q(pk__in=[obj["_id"] for obj in objects])
        # we can apply the other transformation
        logger.debug("Retrieving documents from the main db")
        objects = (
            self._document.objects.using(self.alias)
            .filter(query)
            .only(*self.fields_to_show)
            .order_by(*self.order_by_fields)
        )
        return objects

    def _execute(self):
        logger.debug(
            json.dumps(
                self.filters + self.projections + self.aggregations,
                indent=2,
                default=str,
            )
        )
        # hopefully we can cache the result, or by ram or by db
        if self.count_objects:
            return super().aggregate(
                self.filters + self.projections + self.aggregations
            )

        try:
            objects = self.cache.get(self.filters + self.projections)
        # otherwise we have to actually make the query to the real db
        except (self.cache.ExpiredError, self.cache.KeyError):
            # otherwise we do not need to even populate the cache
            objects = self.__get_from_db()

        # we are sure that at this point we have Mongoengine Documents,
        # meaning that we can safely call the aggregate function
        # if we have an aggregation to do, and let mongoengine manage that
        return objects.aggregate(self.aggregations) if self.aggregations else objects

    def scalar(self, *fields):
        qs = self.clone()
        objects = qs._execute()  # pylint: disable=protected-access
        my_objs = []
        for obj in objects:
            my_objs.append(tuple(obj[k] for k in fields))
        return my_objs

    def __len__(self):
        return len(self._execute())

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.start:
                raise NotImplementedError("Slicing with a start index is not supported")
            qs = self.clone()
            qs.projections.append({"$limit": key.stop})
            return qs._execute()  # pylint: disable=protected-access
        if isinstance(key, int):
            objects = self._execute()
            return list(objects)[key]
        raise TypeError(f"{type(key)} is not a valid key")

    def only(self, *fields):
        qs = self.clone()
        qs.fields_to_show.update(fields)
        qs.fields_to_show.update(qs.must_fields_to_show)
        qs.projections.append(
            {
                "$project": {
                    field.replace("__", ".") if qs.index else field: 1
                    for field in qs.fields_to_show
                }
            }
        )
        return qs

    def count(self, with_limit_and_skip=False):
        qs = self.clone()
        qs.count_objects = True
        if qs.filters:
            qs.filters[0]["$search"]["count"] = {"type": "total"}
            # this should not go in the projections because it is not a field of the document
            qs.aggregations.append({"$project": {"meta": "$$SEARCH_META"}})
        else:
            qs.aggregations.append({"$count": "count"})
        cursor = qs._execute()  # pylint: disable=protected-access
        try:
            count = next(cursor)
        except StopIteration:
            return 0
        else:
            logger.debug(count)
            if qs.filters:
                return count["meta"]["count"]["total"]
            return count["count"]

    def filter(self, q_obj=None, **query):  # pylint: disable=arguments-differ
        q = AtlasQ(**query)
        if q_obj:
            if not isinstance(q_obj, AtlasQ) and not isinstance(
                q_obj, AtlasQCombination
            ):
                raise TypeError(f"Please use Atlasq not {type(q_obj)}")
            q &= q_obj
        text_search, aggregations = q.to_query(self._document)
        qs = self.clone()
        filters = (
            [{"$search": {"index": qs.index, "compound": {"filter": [text_search]}}}]
            if text_search
            else []
        )
        qs.filters = filters + aggregations + qs.filters
        return qs

    def aggregate(self, pipeline: List[Dict], *suppl_pipeline, **kwargs):
        qs = self.clone()
        if not isinstance(pipeline, list):
            raise TypeError("Pipeline should be a list")
        qs.aggregations += pipeline
        qs.aggregations += suppl_pipeline
        return qs._execute()  # pylint: disable=protected-access

    def order_by(self, *keys):
        qs = super().order_by(*keys)
        qs.order_by_fields.update(keys)
        qs.projections.append(
            {"$sort": dict(qs._ordering)}  # pylint: disable=protected-access
        )
        return qs
