import datetime
import logging

from typing import Tuple, List, Dict, Any

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
    text_keywords = ["in", "nin", "contains", "icontains", "iwholeword", "wholeword"]
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

    def __init__(self, altas_query):
        self.altas_query = altas_query

    def __exists(self, path: str, positive: bool) -> Dict:
        # false True == true == eq
        # true False == true == eq
        # false false == false == ne
        # true true == false == ne
        return {
            "$match": {
                path: {
                    "$exists": True,
                    "$eq" if positive else "$ne": [None, [], ""],
                }
            }
        }

    def __range(self, path: str, value: Any, keyword: str) -> Dict:
        if value is None:
            return {}
        if isinstance(value, datetime.datetime):
            value = value.replace(minute=0, second=0, microsecond=0)
        return {"range": {"path": path, keyword: value}}

    def __equals(self, path: str, value: Any) -> Dict:
        return {
            "equals": {
                "path": path,
                "value": value,
            }
        }

    def __text(self, path: str, value: Any) -> Dict:
        if not value:
            raise ValueError(f"Text search for {path} cannot be {value}")
        return {
            "text": {"query": value, "path": path},
        }

    def __size(self, path: str, value: int, positive: bool) -> Dict:
        if not isinstance(value, int):
            raise ValueError(f"Size search for {path} must be an int")
        if value != 0:
            raise NotImplementedError(f"Size search for {path} must be 0")
        return self.__exists(path, positive)

    def transform(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
        other_aggregations = []
        affirmative = []
        negative = []

        for key, value in self.altas_query.items():
            # if to_go is positive, we add the element in the positive list
            # if to_go is negative, we add the element in the negative list
            to_go = 1

            key_parts = key.split("__")
            obj = None
            path = ""
            for i, key_part in enumerate(key_parts):
                if key_part not in self.keywords:
                    continue
                else:
                    # the key_part is made of "field__subfield__keywords
                    # meaning that the first time that we find a keyword, we have the entire path
                    if not path:
                        path = ".".join(key_parts[:i])

                keyword = key_part
                if keyword in self.not_converted:
                    raise NotImplementedError(f"Keyword {keyword} not implemented yet")
                if keyword in self.negative_keywords:
                    to_go *= -1

                if keyword in self.exists_keywords:

                    positive = (to_go == 1) ^ value

                    other_aggregations.append(self.__exists(path, positive))
                    break
                elif keyword in self.size_keywords:
                    # it must the last keyword, otherwise we do not support it
                    if i != len(key_parts) - 1:
                        raise NotImplementedError(
                            f"Keyword {keyword} not implemented yet"
                        )
                    positive = to_go == 1
                    other_aggregations.append(self.__size(path, value, positive))
                    break
                elif keyword in self.range_keywords:
                    obj = self.__range(path, value, keyword)
                    break
                elif keyword in self.equals_keywords:
                    obj = self.__equals(path, value)
                    break
                elif keyword in self.text_keywords:
                    obj = self.__text(path, value)
                    break
            else:
                if not path:
                    path = ".".join(key_parts)
                if isinstance(value, bool):
                    obj = self.__equals(path, value)
                else:
                    obj = self.__text(path, value)

            if obj:
                logger.debug(obj)
                affirmative.append(obj) if to_go == 1 else negative.append(obj)

        return affirmative, negative, other_aggregations
