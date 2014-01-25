from unittest import TestLoader, TextTestRunner
from basic import TestMongoDBExtension

suite = TestLoader().loadTestsFromTestCase(TestMongoDBExtension)
TextTestRunner(verbosity=2).run(suite)
