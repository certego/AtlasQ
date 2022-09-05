from typing import Union

from mongoengine import QuerySetManager

from atlasq.queryset import AtlasIndex
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
        save_execution_time: bool = False,
        use_embedded_documents_indexes: bool = True,
    ):
        super().__init__()
        self._index = (
            AtlasIndex(atlas_index, use_embedded_documents_indexes)
            if atlas_index
            else None
        )
        self._save_execution_time = save_execution_time

    def __get__(self, instance, owner):
        queryset = super().__get__(instance, owner)
        if isinstance(queryset, AtlasQuerySet):
            queryset.index = self._index
            queryset.save_execution_time = self._save_execution_time
        return queryset
