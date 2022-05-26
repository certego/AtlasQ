from django.conf import settings
from mongoengine import QuerySetManager

from atlasq.queryset.queryset import AtlasQuerySet


class AtlasManager(QuerySetManager):
    """
    Manager for the Atlas class.
    """

    @property
    def default(self):
        if self.index:
            return AtlasQuerySet
        else:
            res = super().default
            res.cache_expire_in = lambda x, y: x
            res.sort_by_count = lambda x, field: x.aggregate(
                [{"$sortByCount": f"${field}"}]
            )
            return res

    def __init__(self, index: str = settings.ATLAS_INDEX):
        super().__init__()
        self.index = index

    def __get__(self, instance, owner):
        queryset = super().__get__(instance, owner)
        if isinstance(queryset, AtlasQuerySet):
            queryset.index = self.index
        return queryset
