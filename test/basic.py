from copy import copy
from unittest import TestCase, skip
from time import strftime
from pymongo.mongo_client import MongoClient
from scrapy_mongodb_ext import MongoDBExtensionSingleCollection
from scrapy_mongodb_ext.required_config_param_exception import RequiredConfigParam

TEST_COLLECTION = 'MongoDBExtension'

class BaseClassTestMongoDBExtension(MongoDBExtensionSingleCollection):
    def __init__(self, settings):
        super(BaseClassTestMongoDBExtension, self).__init__(settings)

    def insert_doc(self, doc):
        return self.collection.insert(doc)

    def get_doc(self, oid):
        return self.collection.find_one({'_id': oid})


class TestMongoDBExtension(TestCase):
    MONGODB_EXT_SETTINGS = {
        'MONGODB_URI': 'mongodb://localhost',
        'MONGODB_DATABASE': 'test'
    }

    mongo_test_client = MongoClient(MONGODB_EXT_SETTINGS['MONGODB_URI'])

    def setUp(self):
        self.mongo_test_client[self.MONGODB_EXT_SETTINGS['MONGODB_DATABASE']].drop_collection(TEST_COLLECTION);

    def tearDown(self):
        pass

    @staticmethod
    def union_dicts(first, *others):
        """ Perform the union of the dictionaries
            and returns a new dictionary.
            The returned dictonary is a new dictionary
            instance, however the values are not clone
        """
        union = {}
        union.update(first)

        for another in others:
            union.update(another)

        return union

    def test_base_required_settings(self):
        self.assertRaises(RequiredConfigParam, BaseClassTestMongoDBExtension, {'MONGODB_URI': 'mongodb://localhost'})

    def test_required_collection_setting(self):
        self.assertRaises(RequiredConfigParam, BaseClassTestMongoDBExtension, self.MONGODB_EXT_SETTINGS)

    def test_mongodb_connectivity(self):
        base_mongodb_ext = BaseClassTestMongoDBExtension(self.union_dicts(self.MONGODB_EXT_SETTINGS, {'MONGODB_COLLECTION': TEST_COLLECTION}))
        new_doc_oid = base_mongodb_ext.insert_doc({'name': 'Ivan'})
        new_doc = base_mongodb_ext.get_doc(new_doc_oid)
        self.assertEqual(new_doc_oid, new_doc['_id'])
        self.assertEqual('Ivan', new_doc['name'])

