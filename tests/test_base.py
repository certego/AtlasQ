from unittest import TestCase

import mongomock
from mongoengine import connect, disconnect


class TestBaseCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        connect(db="mongoenginetest", mongo_client_class=mongomock.MongoClient)

    @classmethod
    def tearDownClass(cls) -> None:
        disconnect()
