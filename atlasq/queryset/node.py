import logging

from mongoengine.queryset.visitor import QCombination

from typing import Dict, Tuple, List, Union

from mongoengine import Q

from atlasq.queryset.visitor import AtlasSimplificationVisitor, AtlasQueryCompilerVisitor

logger = logging.getLogger(__name__)


class AtlasQ(Q):
    @property
    def operation(self):
        return self.AND

    def to_query(self, document):
        logger.debug(f"to_query {self.__class__.__name__} {document}")
        query = self.accept(AtlasSimplificationVisitor())
        query = query.accept(AtlasQueryCompilerVisitor(document))
        return query

    def _combine(self, other, operation) -> Union["AtlasQ", "AtlasQCombination"]:
        logger.debug(f"_combine {self.__class__.__name__} {other}, {operation}")

        result = super(AtlasQ, self)._combine(other, operation)
        if isinstance(result, QCombination):
            return AtlasQCombination(result.operation, result.children)
        return AtlasQ(**result.query)



class AtlasQCombination(QCombination):

    def _combine(self, other, operation):
        logger.debug(f"_combine {self.__class__.__name__} {other}, {operation}")
        result = super()._combine(other, operation)
        return AtlasQCombination(result.operation, result.children)

    def to_query(self, document) -> Tuple[Dict, List[Dict]]:
        logger.debug(f"to_query {self.__class__.__name__} {document}")
        query = self.accept(AtlasSimplificationVisitor())
        query = query.accept(AtlasQueryCompilerVisitor(document))
        return query

    def accept(self, visitor):
        logger.debug(f"accept {self.__class__.__name__} {visitor}")
        return super().accept(visitor)
