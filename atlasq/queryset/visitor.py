import logging
from typing import Dict, List, Tuple

from mongoengine import Q
from mongoengine.queryset.visitor import QueryCompilerVisitor, SimplificationVisitor

logger = logging.getLogger(__name__)


class AtlasSimplificationVisitor(SimplificationVisitor):
    def visit_combination(self, combination):
        from atlasq.queryset.node import AtlasQ

        logger.debug(f"visit_combination {self.__class__.__name__} {combination}")
        result = super(AtlasSimplificationVisitor, self).visit_combination(combination)
        if isinstance(result, Q) and not isinstance(result, AtlasQ):
            return AtlasQ(**result.query)
        return result

    def visit_query(self, query):
        logger.debug(f"visit_query {self.__class__.__name__} {query}")
        return super(AtlasSimplificationVisitor, self).visit_query(query)


class AtlasQueryCompilerVisitor(QueryCompilerVisitor):
    def visit_combination(self, combination) -> Tuple[Dict, List[Dict]]:
        filters = []
        aggregations = []
        for child in combination.children:
            if child[0]:
                filters.append(child[0])
            aggregations.extend(child[1])

        if combination.operation == combination.OR:
            return {
                "compound": {"should": filters, "minimumShouldMatch": 1}
            }, aggregations
        return {"compound": {"filter": filters}}, aggregations

    def visit_query(self, query) -> Tuple[Dict, List[Dict]]:
        from atlasq.queryset.transform import AtlasTransform

        affirmative, negative, other_aggregations = AtlasTransform(
            query.query
        ).transform()
        result = {}
        if affirmative:
            result.setdefault("compound", {})["filter"] = affirmative
        if negative:
            result.setdefault("compound", {})["mustNot"] = negative
        return result, other_aggregations
