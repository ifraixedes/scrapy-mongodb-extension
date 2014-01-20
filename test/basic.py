from unittest import TestCase, TestLoader, TextTestRunner
from time import strftime
from pymongo.mongo_client import MongoClient
from python_extension import MongoDBExtension

TEST_COLLECTION = 'MongoDBExtension_'

class BasicMongoDBExtension(MongoDBExtension):

    def __init__(self, settings):
       super(MongoDBExtension, self).__init__(settings)
       self.collection = self.db_connection[TEST_COLLECTION]

    def
    def process_item(self, item, spider):
        self.collection.insert(item)
        return item


class TestMongodbExtension(TestCase):
    MONGODB_EXT_SETTINGS = {
        MONGODB_URI = 'mongodb://localhost',
        MONGODB_DATABASE = 'test'
    }

    mongo_test_collection =
        MongoClient(
            '{0}/{1}'.format(MONGODB_EXT_SETTINGS.MONGO_URI, MONGODB_EXT_SETTINGS.MONGODB_DATABASE)
        )[TEST_COLLECTION]

    def setUp(self):
        colletion
        mongo_test_collection.drop_collection()


    def tearDown(self):
        def.setUp(self


