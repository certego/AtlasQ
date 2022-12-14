from typing import Union

from mongoengine import QuerySetManager

from atlasq.queryset.index import AtlasIndex
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
        return res

    def __init__(
        self,
        atlas_index: Union[str, None],
    ):
        super().__init__()
        self._index = AtlasIndex(atlas_index) if atlas_index else None

    def __get__(self, instance, owner):
        queryset = super().__get__(instance, owner)
        if isinstance(queryset, AtlasQuerySet):
            queryset.index = self._index
        return queryset
