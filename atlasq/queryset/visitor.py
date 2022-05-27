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
    def _visit_combination_and(self, combination) -> Tuple[Dict, List[Dict]]:
        affirmatives = []
        negatives = []
        aggregations = []

        for child in combination.children:
            filters, child_aggregations = child
            if "compound" in filters:
                if "filter" in filters["compound"]:
                    affirmatives.extend(filters["compound"]["filter"])
                if "mustNot" in filters["compound"]:
                    negatives.extend(filters["compound"]["mustNot"])
            aggregations.extend(child_aggregations)
        result = {"compound": {}}
        if affirmatives:
            result["compound"]["filter"] = affirmatives
        if negatives:
            result["compound"]["mustNot"] = negatives
        return result, aggregations

    def _visit_combination_or(self, combination) -> Tuple[Dict, List[Dict]]:
        aggregations = []
        children_results = []
        for child in combination.children:
            filters, aggregations = child
            children_results.append(filters)
            aggregations.extend(aggregations)
        return {
            "compound": {"should": children_results, "minimumShouldMatch": 1}
        }, aggregations

    def visit_combination(self, combination) -> Tuple[Dict, List[Dict]]:
        if combination.operation == combination.AND:
            return self._visit_combination_and(combination)
        return self._visit_combination_or(combination)

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
