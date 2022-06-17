from unittest import TestCase

from mongoengine import connect, disconnect


class TestBaseCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.db_name = "mongoenginetest"
        connect(cls.db_name, host="mongomock://localhost")

    @classmethod
    def tearDownClass(cls) -> None:
        disconnect()
