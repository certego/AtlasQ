import datetime
import hashlib
import json
import logging
from typing import Any, Dict, List, Tuple, Union

from mongoengine import Document, QuerySet, get_db
from mongoengine.context_managers import switch_collection, switch_db
from pymongo.collection import Collection

logger = logging.getLogger(__name__)


class _AtlasCache:
    class ExpiredError(Exception):
        def __init__(self, key):
            super().__init__(f"Cache {key} expired")

    class KeyError(KeyError):
        def __init__(self, key):
            super().__init__(f"Cache key {key} not found")

    def __init__(self, document: Document, collection: Collection, **kwargs):
        self._document = document
        self._collection = collection
        self._max_minutes = 30

    def _aggregations_to_key(self, aggregations: List[Dict]) -> str:
        digest = hashlib.sha256()
        for aggregation in aggregations:
            digest.update(
                json.dumps(aggregation, sort_keys=True, default=str).encode("utf-8")
            )
        return digest.hexdigest()

    def get(self, aggregations: List[Dict], force: bool = False) -> QuerySet:

        raise NotImplementedError()

    def set(self, aggregations: List[Dict], value: QuerySet, max_minutes: int) -> None:

        raise NotImplementedError()

    def remove(self, aggregations: List[Dict]) -> None:
        raise NotImplementedError()


class AtlasDbCache(_AtlasCache):
    def __init__(
        self,
        document: Document,
        collection: Collection,
        db_connection_alias: str,
        **kwargs,
    ):
        super().__init__(document, collection, **kwargs)
        self._db_connection_alias = db_connection_alias
        self.db_name = get_db(self._db_connection_alias).name

    def get_collection_name(self, aggregations: List[Dict]) -> str:
        key = self._aggregations_to_key(aggregations)
        return f"{self._document._meta['collection']}_{key}"  # pylint: disable=protected-access

    def get_collection_expiration(
        self, collection: Document
    ) -> Union[datetime.datetime, None]:
        return collection._meta.get(  # pylint: disable=protected-access
            "expire_at", None
        )

    def remove(self, aggregations: List[Dict]) -> None:
        with switch_db(
            self._document, self._db_connection_alias
        ) as OtherDbCollection:  # pylint: disable=invalid-name
            with switch_collection(
                OtherDbCollection, self.get_collection_name(aggregations)
            ) as OtherCollection:  # pylint: disable=invalid-name
                OtherCollection.drop_collection()
                if (
                    "expire_at"
                    in OtherCollection._meta  # pylint: disable=protected-access
                ):
                    del OtherCollection._meta[  # pylint: disable=protected-access
                        "expire_at"
                    ]

    def get(self, aggregations: List[Dict], force: bool = False) -> QuerySet:
        key = self._aggregations_to_key(aggregations)
        collection = self.get_collection_name(aggregations)
        with switch_db(
            self._document, self._db_connection_alias
        ) as OtherDbCollection:  # pylint: disable=invalid-name
            with switch_collection(
                OtherDbCollection, collection
            ) as OtherCollection:  # pylint: disable=invalid-name
                expire_at = self.get_collection_expiration(OtherCollection)
                if not expire_at:
                    logger.debug(f"Db Cache miss for {key}")
                    raise self.KeyError(key)
                if force or expire_at > datetime.datetime.now():
                    logger.debug(f"Db Cache hit for {key}")
                    return OtherCollection.objects.all()
                logger.debug(f"Db Cache expired for {key}")
                OtherCollection.drop_collection()
                raise self.ExpiredError(key)

    def set(self, aggregations: List[Dict], value: QuerySet, max_minutes: int) -> None:
        raise NotImplementedError()

    def set_collection_expiration(
        self, aggregations: List[Dict], max_minutes: int
    ) -> None:
        key = self._aggregations_to_key(aggregations)
        collection = self.get_collection_name(aggregations)
        with switch_db(
            self._document, self._db_connection_alias
        ) as OtherDbCollection:  # pylint: disable=invalid-name
            with switch_collection(
                OtherDbCollection, collection
            ) as OtherCollection:  # pylint: disable=invalid-name
                OtherCollection._meta[  # pylint: disable=protected-access
                    "expire_at"
                ] = datetime.datetime.now() + datetime.timedelta(minutes=max_minutes)

        logger.debug(f"Db Cache expiration set for {key}")


class AtlasRamCache(_AtlasCache):
    def __init__(self, document: Document, collection: Collection, **kwargs):
        super().__init__(document, collection, **kwargs)
        self._cache: Dict[str, Tuple[datetime.datetime, QuerySet]] = {}

    def remove(self, aggregations: List[Dict]) -> None:
        key = self._aggregations_to_key(aggregations)
        if key in self._cache:
            del self._cache[key]

    def set(self, aggregations: List[Dict], value: Any, max_minutes: int) -> None:
        expire_date = datetime.datetime.now() + datetime.timedelta(minutes=max_minutes)
        key = self._aggregations_to_key(aggregations)
        self._cache[key] = (expire_date, value)
        logger.debug(f"Ram Cache set for {key}")

    def get(self, aggregations: List[Dict], force: bool = False) -> Any:
        key = self._aggregations_to_key(aggregations)
        if key in self._cache:
            submission_time, objects = self._cache[key]
            if force or datetime.datetime.now() <= submission_time:
                logger.debug(f"Ram Cache hit for {key}")
                return objects

            logger.debug(f"Ram Cache expired for {key}")
            del self._cache[key]
            raise self.ExpiredError(key)
        logger.debug(f"Ram Cache miss for {key}")
        raise self.KeyError(key)


class AtlasCache(AtlasDbCache, AtlasRamCache):
    def remove(self, aggregations: List[Dict]) -> None:
        AtlasRamCache.remove(self, aggregations)
        AtlasDbCache.remove(self, aggregations)

    def get(self, aggregations: List[Dict], force: bool = False) -> QuerySet:
        # we first try to use the ram cache, if that does not work, we try the db cache
        try:
            # we have a generator here
            objects = AtlasRamCache.get(self, aggregations, force)
        except (self.KeyError, self.ExpiredError):
            objects = AtlasDbCache.get(self, aggregations, force)
            # if the db cache worked, we set the ram cache
            # we need to copy the expiration time
            with switch_db(
                self._document, self._db_connection_alias
            ) as OtherDbCollection:  # pylint: disable=invalid-name
                with switch_collection(
                    OtherDbCollection, self.get_collection_name(aggregations)
                ) as OtherCollection:  # pylint: disable=invalid-name
                    expire_in = self.get_collection_expiration(OtherCollection)
            # we calculate the expiration time and round it to the nearest minute
            diff = expire_in - datetime.datetime.now()
            minutes_rounded = int((diff.total_seconds() + 60 / 2) // 60)
            # and finally we can set it with a generator
            AtlasRamCache.set(self, aggregations, objects, minutes_rounded)
        return objects

    def set(self, aggregations: List[Dict], value: QuerySet, max_minutes: int) -> None:
        raise NotImplementedError()
