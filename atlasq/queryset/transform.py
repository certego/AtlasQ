import datetime
import logging
from typing import Any, Dict, List, Tuple, Union

from mongoengine import QuerySet

from atlasq.queryset.exceptions import AtlasFieldError, AtlasIndexFieldError
from atlasq.queryset.index import AtlasIndex

logger = logging.getLogger(__name__)


class AtlasTransform:
    keywords = [
        "ne",
        "lt",
        "gt",
        "lte",
        "gte",
        "in",
        "nin",
        "all",
        "size",
        "exists",
        "exact",
        "iexact",
        "contains",
        "icontains",
        "startswith",
        "istartswith",
        "iwholeword",
        "wholeword",
        "not",
        "mod",
        "regex",
        "iregex",
        "match",
        "is",
    ]
    negative_keywords = ["ne", "nin", "not"]
    exists_keywords = ["exists"]
    range_keywords = ["gt", "gte", "lt", "lte"]
    equals_keywords = ["exact", "iexact", "eq"]
    text_keywords = [
        "contains",
        "icontains",
        "iwholeword",
        "wholeword",
    ]
    size_keywords = ["size"]
    not_converted = [
        "all",
        "istartswith",
        "startswith",
        "contains",
        "icontains",
        "mod",
        "regex",
        "iregex",
        "match",
    ]

    def __init__(self, atlas_query):
        self.atlas_query = atlas_query

    def _exists(self, path: str, empty: bool) -> Dict:
        # false True == true == eq
        # true False == true == eq
        # false false == false == ne
        # true true == false == ne
        return {
            "$match": {
                path: {
                    "$exists": True,
                    "$eq" if empty else "$ne": [None, [], ""],
                }
            }
        }

    def _range(
        self, path: str, value: Union[int, datetime.datetime], keyword: str
    ) -> Dict:
        if keyword not in self.range_keywords:
            raise AtlasFieldError(
                f"Range search for {path} must be {self.range_keywords}, not {keyword}"
            )
        if isinstance(value, datetime.datetime):
            value = value.replace(minute=0, second=0, microsecond=0)
        elif isinstance(value, int):
            pass
        else:
            raise AtlasFieldError(
                f"Range search for {path} must have a value of datetime or integer"
            )
        return {"range": {"path": path, keyword: value}}

    def _equals(self, path: str, value: Any) -> Dict:
        return {
            "equals": {
                "path": path,
                "value": value,
            }
        }

    def _text(self, path: str, value: Any) -> Dict:
        if not value:
            raise AtlasFieldError(f"Text search for {path} cannot be {value}")
        return {
            "text": {"query": value, "path": path},
        }

    def _size(self, path: str, value: int, operator: str) -> Dict:
        if not isinstance(value, int):
            raise AtlasFieldError(f"Size search for {path} must be an int")
        if value != 0:
            raise NotImplementedError(f"Size search for {path} must be 0")
        if operator not in ["eq", "ne"]:
            raise NotImplementedError(f"Size search for {path} must be eq or ne")
        empty = operator == "eq"
        return self._exists(path, empty)

    def _ensure_keyword_is_indexed(self, atlas_index: AtlasIndex, keyword: str) -> None:
        if atlas_index.ensured:
            if not atlas_index.ensure_keyword_is_indexed(keyword):
                raise AtlasIndexFieldError(
                    f"The keyword {keyword} is not indexed in {atlas_index.index}"
                )

    def transform(
        self, atlas_index: AtlasIndex
    ) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        other_aggregations = []
        affirmative = []
        negative = []

        for key, value in self.atlas_query.items():
            # if to_go is positive, we add the element in the positive list
            # if to_go is negative, we add the element in the negative list
            to_go = 1
            if isinstance(value, QuerySet):
                logger.debug(
                    "Casting queryset to list, otherwise the aggregation will fail"
                )
                value = list(value)
            key_parts = key.split("__")
            obj = None
            path = ""
            for i, key_part in enumerate(key_parts):
                if key_part not in self.keywords:
                    continue
                # the key_part is made of "field__subfield__keywords
                # meaning that the first time that we find a keyword, we have the entire path
                if not path:
                    path = ".".join(key_parts[:i])
                    self._ensure_keyword_is_indexed(atlas_index, path)

                keyword = key_part
                if keyword in self.not_converted:
                    raise NotImplementedError(f"Keyword {keyword} not implemented yet")
                if keyword in self.negative_keywords:
                    to_go *= -1

                if keyword in self.exists_keywords:

                    empty = (to_go == 1) ^ value

                    other_aggregations.append(self._exists(path, empty))
                    break
                if keyword in self.size_keywords:
                    # it must the last keyword, otherwise we do not support it
                    if i != len(key_parts) - 1:
                        raise NotImplementedError(
                            f"Keyword {keyword} not implemented yet"
                        )
                    other_aggregations.append(
                        self._size(path, value, "eq" if to_go == 1 else "ne")
                    )
                    break
                if keyword in self.range_keywords:
                    obj = self._range(path, value, keyword)
                    break
                if keyword in self.equals_keywords:
                    obj = self._equals(path, value)
                    break
                if keyword in self.text_keywords:
                    obj = self._text(path, value)
                    break
            else:
                if not path:
                    path = ".".join(key_parts)
                    self._ensure_keyword_is_indexed(atlas_index, path)
                if isinstance(value, bool):
                    obj = self._equals(path, value)

                else:
                    obj = self._text(path, value)

            if obj:
                logger.debug(obj)
                if to_go == 1:
                    affirmative.append(obj)
                else:
                    negative.append(obj)

        return affirmative, negative, other_aggregations
