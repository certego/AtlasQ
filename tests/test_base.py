from unittest import TestCase

import mongomock
from mongoengine import connect, disconnect


class TestBaseCase(TestCase):

    db_name = "mongoenginetest"

    @classmethod
    def setUpClass(cls) -> None:
        connect(db=cls.db_name, mongo_client_class=mongomock.MongoClient)

    @classmethod
    def tearDownClass(cls) -> None:
        disconnect()
