import logging
from typing import Dict, List, Tuple

from mongoengine import Q
from mongoengine.queryset.visitor import QueryCompilerVisitor, SimplificationVisitor

from atlasq.queryset.index import AtlasIndex

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
    def __init__(self, document, atlas_index: AtlasIndex):
        super(AtlasQueryCompilerVisitor, self).__init__(document)
        self.atlas_index = atlas_index

    def _visit_combination_and(self, combination) -> Tuple[Dict, List[Dict]]:
        affirmatives = []
        negatives = []
        aggregations = []
        for child in combination.children:
            filters, *child_aggregations = child
            try:
                filters = filters["$search"]
            except KeyError:
                # in case we return just aggregations, the filters should be empty
                child_aggregations = [filters] + child_aggregations
                filters = {}
            if "compound" in filters:
                if "filter" in filters["compound"]:
                    affirmatives.extend(filters["compound"]["filter"])
                if "mustNot" in filters["compound"]:
                    negatives.extend(filters["compound"]["mustNot"])
                if "should" in filters["compound"]:
                    filters.pop("index")
                    affirmatives.append(filters)
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
            filters, *child_aggregations = child
            try:
                filters = filters["$search"]
            except KeyError:
                # in case we return just aggregations, the filters should be empty
                child_aggregations = [filters] + child_aggregations
            else:
                filters.pop("index")
                children_results.append(filters)
            aggregations.extend(child_aggregations)
        return {
            "compound": {"should": children_results, "minimumShouldMatch": 1}
        }, aggregations

    def visit_combination(self, combination) -> List[Dict]:
        if combination.operation == combination.AND:
            filters, aggregations = self._visit_combination_and(combination)
        else:
            filters, aggregations = self._visit_combination_or(combination)
        filters["index"] = self.atlas_index.index
        result = [{"$search": filters}] + aggregations
        return result

    def visit_query(self, query) -> List[Dict]:
        from atlasq.queryset.transform import AtlasTransform

        affirmative, negative, aggregations = AtlasTransform(
            query.query, self.atlas_index
        ).transform()
        filters = {}
        if affirmative:
            filters.setdefault("compound", {})["filter"] = affirmative
        if negative:
            filters.setdefault("compound", {})["mustNot"] = negative
        result = []
        if filters:
            filters["index"] = self.atlas_index.index
            result += [{"$search": filters}]
        result += aggregations
        return result
