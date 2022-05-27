from unittest import TestCase

from mongoengine import connect, disconnect


class TestBaseCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        connect("mongoenginetest", host="mongomock://localhost")

    @classmethod
    def tearDownClass(cls) -> None:
        disconnect()
