import datetime
import logging
from typing import Any, Dict, List, Tuple, Union

from bson import ObjectId
from mongoengine import QuerySet

from atlasq.queryset.exceptions import AtlasFieldError, AtlasIndexFieldError
from atlasq.queryset.index import AtlasIndex, AtlasIndexType

logger = logging.getLogger(__name__)


class AtlasTransform:

    id_keywords = [
        "pk",
        "id",
        "_id",
    ]

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
    equals_keywords = []
    text_keywords = [
        "contains",
        "icontains",
        "iwholeword",
        "wholeword",
        "exact",
        "iexact",
        "eq",
    ]
    regex_keywords = ["regex", "iregex"]
    size_keywords = ["size"]
    not_converted = [
        "all",
        "istartswith",
        "startswith",
        "contains",
        "icontains",
        "mod",
        "match",
    ]

    def __init__(self, atlas_query, atlas_index: AtlasIndex):
        self.atlas_query = atlas_query
        self.atlas_index = atlas_index

    def _regex(self, path: str, value: str):
        return {"regex": {"query": value, "path": path}}

    def _embedded_document(self, path: str, content: Dict, positive: bool):
        operator = "must" if positive else "mustNot"
        return {
            "embeddedDocument": {
                "path": path,
                "operator": {"compound": {operator: [content]}},
            }
        }

    def _convert_to_embedded_document(
        self, path: List[str], operator: Dict, positive: bool, start: str = ""
    ):
        element = path.pop(0)
        partial_path = f"{start}.{element}" if start else element
        if not self.atlas_index.ensured:
            return operator
        if (
            self.atlas_index.get_type_from_keyword(partial_path)
            != AtlasIndexType.EMBEDDED_DOCUMENT.value
        ):
            return operator

        if not path:
            return operator

        new_operator = self._convert_to_embedded_document(
            path, operator, start=partial_path, positive=positive
        )
        return self._embedded_document(
            partial_path,
            new_operator,
            True
            if operator != new_operator
            else positive,  # this cover the case of multiple embeddedDocument,
            # where only the last one must be set to negative
        )

    def _exists(self, path: str) -> Dict:
        return {"exists": {"path": path}}

    def _range(
        self, path: str, value: Union[int, datetime.datetime], keywords: List[str]
    ) -> Dict:
        for keyword in keywords:
            if keyword not in self.range_keywords:
                raise AtlasFieldError(
                    f"Range search for {path} must be {self.range_keywords}, not {keyword}"
                )
        if isinstance(value, datetime.datetime):
            value = value.replace(microsecond=0)
        elif isinstance(value, int):
            pass
        else:
            raise AtlasFieldError(
                f"Range search for {path} must have a value of datetime or integer"
            )
        return {"range": {"path": path, **{keyword: value for keyword in keywords}}}

    def _single_equals(self, path, value: Union[ObjectId, bool]):
        if not isinstance(value, (ObjectId, bool)):
            raise AtlasFieldError(
                f"Text search for equals on {path=} cannot be {value}, must be ObjectId or bool"
            )
        return {
            "equals": {
                "path": path,
                "value": value,
            }
        }

    def _equals(
        self, path: str, value: Union[List[Union[ObjectId, bool]], ObjectId, bool]
    ) -> Dict:
        if isinstance(value, list):

            values = value
            if not values:
                raise AtlasFieldError(
                    f"Text search for equals on {path=} cannot be empty"
                )
            base = {"compound": {"should": [], "minimumShouldMatch": 1}}
            for value in values:
                base["compound"]["should"].append(self._single_equals(path, value))
            return base
        return self._single_equals(path, value)

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
        return {
            "$match": {
                path: {
                    "$exists": True,
                    f"${operator}": [None, [], ""],
                }
            }
        }

    def _ensure_path_is_indexed(self, path: List[str]) -> None:
        start = ""
        for element in path:
            partial_path = f"{start}.{element}" if start else element

            if not self.atlas_index.ensure_keyword_is_indexed(partial_path):
                raise AtlasIndexFieldError(
                    f"The keyword {partial_path} is not indexed in {self.atlas_index.index}"
                )
            start = partial_path

    @staticmethod
    def _cast_to_object_id(
        value: Union[str, ObjectId, List[Union[str, ObjectId]]]
    ) -> Union[ObjectId, List[ObjectId]]:
        if isinstance(value, str):
            value = ObjectId(value)
        elif isinstance(value, list):
            for j in range(len(value)):  # pylint: disable=consider-using-enumerate
                if isinstance(value[j], str):
                    value[j] = ObjectId(value[j])
                elif isinstance(value[j], ObjectId):
                    pass
                else:
                    raise TypeError(f"Wrong type {type(value[j])} for id field")
        elif isinstance(value, ObjectId):
            pass
        else:
            raise TypeError(f"Wrong type {type(value)} for id field")
        return value

    def transform(self) -> Tuple[List[Dict], List[Dict], List[Dict]]:
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
            for i, keyword in enumerate(key_parts):

                if keyword in self.id_keywords:
                    keyword = "_id"
                    key_parts[i] = keyword
                    value = self._cast_to_object_id(value)
                if keyword not in self.keywords:
                    continue
                # the key_part is made of "field__subfield__keywords
                # meaning that the first time that we find a keyword, we have the entire path
                if not path:
                    path = ".".join(key_parts[:i])

                if keyword in self.not_converted:
                    raise NotImplementedError(f"Keyword {keyword} not implemented yet")
                if keyword in self.negative_keywords:
                    to_go *= -1

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
                if keyword in self.exists_keywords:
                    if value is False:
                        to_go *= -1
                    obj = self._exists(path)
                    break

                if keyword in self.range_keywords:
                    obj = self._range(path, value, [keyword])
                    break
                if keyword in self.equals_keywords:
                    obj = self._equals(path, value)
                    break
                if keyword in self.text_keywords:
                    obj = self._text(path, value)
                    break
                if keyword in self.regex_keywords:
                    obj = self._regex(path, value)
                    break
            else:
                if not path:
                    path = ".".join(key_parts)
                if isinstance(value, list):
                    value_to_check = value[0]
                else:
                    value_to_check = value
                if isinstance(value_to_check, (bool, ObjectId)):
                    obj = self._equals(path, value)
                elif isinstance(value_to_check, int):
                    obj = self._range(path, value, ["gte", "lte"])
                else:
                    obj = self._text(path, value)

            if obj:
                if self.atlas_index.ensured:
                    self._ensure_path_is_indexed(path.split("."))
                # we are wrapping the result to an embedded document
                converted = self._convert_to_embedded_document(
                    path.split("."), obj, positive=to_go == 1
                )
                if obj != converted:
                    # we have an embedded object
                    # the mustNot is done inside the embedded document clause
                    affirmative = self.merge_embedded_documents(converted, affirmative)
                else:
                    if to_go == 1:
                        affirmative.append(converted)
                    else:
                        negative.append(converted)
        if other_aggregations:
            logger.warning(
                "CARE! You are generating a query that uses other aggregations other than text search!"
                f" Aggregations generated are {other_aggregations}"
            )
        return affirmative, negative, other_aggregations

    @staticmethod
    def merge_embedded_documents(obj: Dict, list_of_obj: List[Dict]) -> List[Dict]:
        list_of_obj = list(list_of_obj)  # I hate function that change stuff in place
        assert "embeddedDocument" in obj
        assert "path" in obj["embeddedDocument"]
        assert "operator" in obj["embeddedDocument"]
        assert "compound" in obj["embeddedDocument"]["operator"]
        # path that we want merge
        path = obj["embeddedDocument"]["path"]
        keys = list(obj["embeddedDocument"]["operator"]["compound"].keys())
        assert len(keys) == 1
        operator = keys[0]  # values could be (must, mustNot)
        # the actual query
        content = obj["embeddedDocument"]["operator"]["compound"][operator]
        for already_present_obj in list_of_obj:
            # we have added an object that is not actually an embedded object, nothing to do
            if "embeddedDocument" not in already_present_obj:
                continue
            # we check for a correspondence
            if path == already_present_obj["embeddedDocument"]["path"]:
                # we merge the objects
                already_present_obj["embeddedDocument"]["operator"][
                    "compound"
                ].setdefault(operator, []).extend(content)
                # we can exit since we are sure that it can be only 1 hit for path
                # if this method is called at every embedded object
                break
        # otherwise we just add the object if no hit has been found
        else:
            list_of_obj.append(obj)
        return list_of_obj
