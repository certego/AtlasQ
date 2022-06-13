import logging
from typing import Dict, List, Tuple, Union

from mongoengine import Q
from mongoengine.queryset.visitor import QCombination

from atlasq.queryset.index import AtlasIndex
from atlasq.queryset.visitor import (
    AtlasQueryCompilerVisitor,
    AtlasSimplificationVisitor,
)

logger = logging.getLogger(__name__)


class AtlasQ(Q):
    @property
    def operation(self):
        return self.AND

    def to_query(  # pylint: disable=arguments-differ
        self, document, atlas_index: Union[None, AtlasIndex] = None
    ) -> Tuple[Dict, List[Dict]]:
        if AtlasIndex is None:
            return super().to_query(document)
        logger.debug(f"to_query {self.__class__.__name__} {document}")
        query = self.accept(AtlasSimplificationVisitor())
        query = query.accept(AtlasQueryCompilerVisitor(document, atlas_index))
        return query

    def _combine(self, other, operation) -> Union["AtlasQ", "AtlasQCombination"]:
        logger.debug(f"_combine {self.__class__.__name__} {other}, {operation}")

        result = super(AtlasQ, self)._combine(other, operation)
        if isinstance(result, QCombination):
            return AtlasQCombination(result.operation, result.children)
        return AtlasQ(**result.query)


class AtlasQCombination(QCombination):
    def __bool__(self):
        return any(bool(child) for child in self.children)

    def _combine(self, other, operation):
        logger.debug(f"_combine {self.__class__.__name__} {other}, {operation}")
        result = super()._combine(other, operation)
        return AtlasQCombination(result.operation, result.children)

    def to_query(  # pylint: disable=arguments-differ
        self, document, atlas_index: Union[None, AtlasIndex] = None
    ) -> Tuple[Dict, List[Dict]]:
        if AtlasIndex is None:
            return super().to_query(document)
        logger.debug(f"to_query {self.__class__.__name__} {document}")
        query = self.accept(AtlasSimplificationVisitor())
        query = query.accept(AtlasQueryCompilerVisitor(document, atlas_index))
        return query

    def accept(self, visitor):
        logger.debug(f"accept {self.__class__.__name__} {visitor}")
        return super().accept(visitor)
