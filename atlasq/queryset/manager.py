from typing import Union

from mongoengine import QuerySetManager

from atlasq.queryset.queryset import AtlasQuerySet


class AtlasManager(QuerySetManager):
    """
    Manager for the Atlas class.
    """

    @property
    def default(self):
        if self._index:
            return AtlasQuerySet
        res = super().default
        res.cache_expire_in = lambda x, y: x
        res.sort_by_count = lambda x, field: x.aggregate(
            [{"$sortByCount": f"${field}"}]
        )
        return res

    def __init__(self, index: Union[str, None], cache_db_alias: str):
        super().__init__()
        self._index = index
        self._cache_db_alias = cache_db_alias

    def __get__(self, instance, owner):
        queryset = super().__get__(instance, owner)
        if isinstance(queryset, AtlasQuerySet):
            queryset.index = self._index
            queryset.cache = self._cache_db_alias
        return queryset
