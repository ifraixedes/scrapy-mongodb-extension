from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference
from scrapy import log
from required_config_param_exception import RequiredConfigParam

VERSION = '0.1.0'

class MongoDBExtension(object):
    """ MongoDB extension class
        The main reason of this class is to be extended to create
        extensions (middlewares or pipelines as well) which use
        a MongoDB connection to perform their operations
        The objects of this class have a class attribute,
        `configuration` (commented below) and an instance attribute
        named db_connection which is the connection object to  MongoDB
        specified in the settings
    """
    """
    Return the default settings tuples collection
    Each tuple define a configuration parameters, some
    of them are applied straightaway to create MongoClient.
    The elements of each tuple specify:
        1. Parameter name used by this class or object instances.
        2. Settings parameter name. The parameter's name use in
         scrapy settings file
        3. If it is required
        4. Default value to use; when 3rd parameter is True, it
            isn't considered, but it must exist (e.g. set to None)
            for all the tuples have the same name of elements
    """
    configuration_specification = (
        ('uri', 'MONGODB_URI', True, None),
        ('fsync', 'MONGODB_FSYNC', False, False),
        ('write_concern', 'MONGODB_REPLICA_SET_W', False, 0),
        ('database', 'MONGODB_DATABASE', True, None),
        ('replica_set', 'MONGODB_REPLICA_SET', False, None)
    )

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        """ Constructor """
        config = self.get_configuration(settings)

        if config['replica_set'] is not None:
            connection = MongoReplicaSetClient(
                config['uri'],
                replicaSet=config['replica_set'],
                w=config['write_concern'],
                fsync=config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            connection = MongoClient(
                config['uri'],
                fsync=config['fsync'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the collection
        self.db_connection = connection[config['database']]
        log.msg('Connected to MongoDB {0} and "{1}" database'.format(
            config['uri'],
            config['database']))

    @classmethod
    def get_configuration(cls, settings):
        """ Return a dictionary with the configuration parameters.
            It checks the required settings and populating the
            optional settings to the specified default values defined
            by `configuration_specification` class attribute.
        """
        config = {}

        for config_param_spec in cls.configuration_specification:
            key, setting, required, default_value = config_param_spec
            if setting in settings:
                config[key] = settings[setting]
            else:
                if (required):
                    raise RequiredConfigParam(key)
                else:
                    config[key] = default_value

        return config

class MongoDBExtensionSingleCollection(MongoDBExtension):
    """ Abstract class which create an instance attribute
        `collection` which is a pymongo collection object
        bound to the collection name provided by the
        `MONGODB_COLLECTION` settings parameter
    """
    def __init__(self, settings):
        if 'MONGODB_COLLECTION' not in settings:
            raise RequiredConfigParam('MONGODB_COLLECTION')

        super(MongoDBExtensionSingleCollection, self).__init__(settings)
        self.collection = self.db_connection[settings['MONGODB_COLLECTION']]
